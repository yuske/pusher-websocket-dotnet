using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Threading.Tasks;
using vtortola.WebSockets;
using vtortola.WebSockets.Rfc6455;

namespace PusherClient
{
    internal class Connection
    {
        private WebSocketClient _client;
        private WebSocket _websocket;
        private Task _task;
        private CancellationTokenSource _cts;
        private readonly SemaphoreSlim _lockObject = new SemaphoreSlim(1);
        private readonly string _url;
        private readonly Pusher _pusher;

        public event ErrorEventHandler Error;
        public event ConnectedEventHandler Connected;
        public event ConnectionStateChangedEventHandler ConnectionStateChanged;

        internal string SocketID { get; private set; }

        internal ConnectionState State { get; private set; } = ConnectionState.Initialized;

        public Connection(Pusher pusher, string url)
        {
            _url = url;
            _pusher = pusher;
        }

        // the method is not thread-safe
        public void Run()
        {
            Pusher.Trace.TraceEvent(TraceEventType.Information, 0, $"Connecting to: {_url}");

            ChangeState(ConnectionState.Connecting);

            _cts = new CancellationTokenSource();
            _task = Task.Run(Receive, _cts.Token);
        }

        // the method is not thread-safe
        public void Stop()
        {
            if (StopAsync().Wait(5000))
            {
                Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "The WebSocket task is not stopped.");
            }
        }

        public void Send(string message)
        {
            SendAsync(message).Wait();
        }

        public async Task StopAsync()
        {
            if (_cts == null || _task == null)
                return;

            _cts.Cancel();
            await _task;
            _task.Dispose();
            _cts.Dispose();
            _cts = null;
            _task = null;
        }

        public async Task SendAsync(string message)
        {
            if (State != ConnectionState.Connected)
            {
                Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "The WebSocket state is not connected.");
                return;
            }

            await _lockObject.WaitAsync(_cts.Token);
            try
            {
                Pusher.Trace.TraceEvent(TraceEventType.Information, 0, "Sending: " + message);
                Debug.WriteLine("Sending: " + message);

                var data = Encoding.UTF8.GetBytes(message);
                using (var writer = _websocket.CreateMessageWriter(WebSocketMessageType.Text))
                {
                    await writer.WriteAsync(data, 0, data.Length, _cts.Token);
                }
            }
            catch (Exception e)
            {
                Pusher.Trace.TraceEvent(TraceEventType.Error, 0, "Error sending a data by WebSocket. " + e);
            }
            finally
            {
                _lockObject.Release();
            }
        }

        private async Task Receive()
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                if (_websocket == null || !_websocket.IsConnected)
                {
                    Pusher.Trace.TraceEvent(TraceEventType.Information, 0, "Trying to connect by WebSocket.");
                    if (!await Connect())
                    {
                        // try to reconnect in 5 sec timeout
                        await Task.Delay(TimeSpan.FromSeconds(5), _cts.Token);
                    }
                }
                else
                {
                    try
                    {
                        using (var result = await _websocket.ReadMessageAsync(_cts.Token))
                        {
                            if (result.MessageType == WebSocketMessageType.Binary)
                            {
                                Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "Have got a binary message by WebSocket.");
                            }
                            else
                            {
                                using (var reader = new StreamReader(result, Encoding.UTF8))
                                {
                                    HandleMessage(await reader.ReadToEndAsync());
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        await Close();
                        Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "Error WebSocket: " + e);
                    }
                }
            }

            await Close();
        }

        private async Task<bool> Connect()
        {
            try
            {
                await _lockObject.WaitAsync(_cts.Token);
                try
                {
                    if (State != ConnectionState.Connecting)
                    {
                        ChangeState(ConnectionState.WaitingToReconnect);
                    }

                    _websocket?.Dispose();
                    
                    // create new connection
                    var bufferSize = 1024 * 8; // 8KiB
                    var bufferPoolSize = 100 * bufferSize; // 800KiB pool

                    var options = new WebSocketListenerOptions
                    {
                        // set send buffer size (optional but recommended)
                        SendBufferSize = bufferSize,
                        // set buffer manager for buffers re-use (optional but recommended)
                        BufferManager = BufferManager.CreateBufferManager(bufferPoolSize, bufferSize),
                        // set timeout for slow connections
                        NegotiationTimeout = TimeSpan.FromSeconds(20)
                    };

                    // register RFC6455 protocol implementation (required)
                    options.Standards.RegisterRfc6455();

                    // configure tcp transport (optional)
                    options.Transports.ConfigureTcp(tcp =>
                    {
                        tcp.ReceiveBufferSize = bufferSize;
                        tcp.SendBufferSize = bufferSize;
                    });

                    _client = new WebSocketClient(options);
                    _websocket = await _client.ConnectAsync(new Uri(_url), _cts.Token);
                    Pusher.Trace.TraceEvent(TraceEventType.Information, 0, "Websocket opened OK.");
                }
                finally
                {
                    _lockObject.Release();
                }

                return true;
            }
            catch (Exception e)
            {
                Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "The WebSocket connection attempt throws the exception. " + e);
                return false;
            }
        }

        private async Task Close()
        {
            await _lockObject.WaitAsync();
            try
            {
                await _websocket.CloseAsync();
                await _client.CloseAsync();
                Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "Websocket connection has been closed");
            }
            catch (Exception e)
            {
                Pusher.Trace.TraceEvent(TraceEventType.Error, 0, "Error WebSocket closing. " + e);
            }
            finally
            {
                _lockObject.Release();
                ChangeState(ConnectionState.Disconnected);
            }
        }

        private void ChangeState(ConnectionState state)
        {
            State = state;
            ConnectionStateChanged?.Invoke(this, State);
        }

        private void RaiseError(PusherException error)
        {
            // if a handler is registerd, use it, otherwise just trace. No code can catch exception here if thrown.
            var handler = Error;
            if (handler != null)
            {
                handler(this, error);
            }
            else
            {
                Pusher.Trace.TraceEvent(TraceEventType.Error, 0, error.ToString());
            }
        }

        private void HandleMessage(string receivedMessage)
        {
            Pusher.Trace.TraceEvent(TraceEventType.Information, 0, "Websocket message received: " + receivedMessage);

            Debug.WriteLine(receivedMessage);

            // DeserializeAnonymousType will throw and error when an error comes back from pusher
            // It stems from the fact that the data object is a string normally except when an error is sent back
            // then it's an object.

            // bad:  "{\"event\":\"pusher:error\",\"data\":{\"code\":4201,\"message\":\"Pong reply not received\"}}"
            // good: "{\"event\":\"pusher:error\",\"data\":\"{\\\"code\\\":4201,\\\"message\\\":\\\"Pong reply not received\\\"}\"}";

            var jObject = JObject.Parse(receivedMessage);

            if (jObject["data"] != null && jObject["data"].Type != JTokenType.String)
                jObject["data"] = jObject["data"].ToString(Formatting.None);

            string jsonMessage = jObject.ToString(Formatting.None);
            var template = new { @event = String.Empty, data = String.Empty, channel = String.Empty };

            //var message = JsonConvert.DeserializeAnonymousType(e.Message, template);
            var message = JsonConvert.DeserializeAnonymousType(jsonMessage, template);

            _pusher.EmitEvent(message.@event, message.data);

            if (message.@event.StartsWith("pusher"))
            {
                // Assume Pusher event
                switch (message.@event)
                {
                    case Constants.ERROR:
                        ParseError(message.data);
                        break;

                    case Constants.CONNECTION_ESTABLISHED:
                        ParseConnectionEstablished(message.data);
                        break;

                    case Constants.CHANNEL_SUBSCRIPTION_SUCCEEDED:

                        if (_pusher.Channels.ContainsKey(message.channel))
                        {
                            var channel = _pusher.Channels[message.channel];
                            channel.SubscriptionSucceeded(message.data);
                        }

                        break;

                    case Constants.CHANNEL_SUBSCRIPTION_ERROR:

                        RaiseError(new PusherException("Error received on channel subscriptions: " + receivedMessage, ErrorCodes.SubscriptionError));
                        break;

                    case Constants.CHANNEL_MEMBER_ADDED:

                        // Assume channel event
                        if (_pusher.Channels.ContainsKey(message.channel))
                        {
                            var channel = _pusher.Channels[message.channel];

                            if (channel is PresenceChannel)
                            {
                                ((PresenceChannel)channel).AddMember(message.data);
                                break;
                            }
                        }

                        Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "Received a presence event on channel '" + message.channel + "', however there is no presence channel which matches.");
                        break;

                    case Constants.CHANNEL_MEMBER_REMOVED:

                        // Assume channel event
                        if (_pusher.Channels.ContainsKey(message.channel))
                        {
                            var channel = _pusher.Channels[message.channel];

                            if (channel is PresenceChannel)
                            {
                                ((PresenceChannel)channel).RemoveMember(message.data);
                                break;
                            }
                        }

                        Pusher.Trace.TraceEvent(TraceEventType.Warning, 0, "Received a presence event on channel '" + message.channel + "', however there is no presence channel which matches.");
                        break;

                }
            }
            else
            {
                // Assume channel event
                if (_pusher.Channels.ContainsKey(message.channel))
                    _pusher.Channels[message.channel].EmitEvent(message.@event, message.data);
            }
        }

        private void ParseConnectionEstablished(string data)
        {
            var template = new { socket_id = String.Empty };
            var message = JsonConvert.DeserializeAnonymousType(data, template);
            SocketID = message.socket_id;

            ChangeState(ConnectionState.Connected);

            Connected?.Invoke(this);
        }

        private void ParseError(string data)
        {
            var template = new { message = String.Empty, code = (int?) null };
            var parsed = JsonConvert.DeserializeAnonymousType(data, template);

            ErrorCodes error = ErrorCodes.Unkown;

            if (parsed.code != null && Enum.IsDefined(typeof(ErrorCodes), parsed.code))
            {
                error = (ErrorCodes)parsed.code;
            }

            RaiseError(new PusherException(parsed.message, error));
        }
    }
}
