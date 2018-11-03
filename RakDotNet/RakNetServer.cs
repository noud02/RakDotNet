using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace RakDotNet
{
    public class RakNetServer
    {
        public event Func<byte[], IPEndPoint, Task> PacketReceived;
        public event Func<IPEndPoint, Task> NewConnection;
        public event Func<IPEndPoint, Task> Disconnection;

        private readonly UdpClient _udp;
        private readonly Dictionary<IPEndPoint, ReliabilityLayer> _connections;
        private readonly byte[] _password;
        private readonly IPEndPoint _endpoint;
        private readonly DateTimeOffset _startTime;

        private Task _task;
        private bool _active;

        public DateTimeOffset StartTime => _startTime;

        public RakNetServer(int port, byte[] password)
        {
            _udp = new UdpClient(port);
            _connections = new Dictionary<IPEndPoint, ReliabilityLayer>();
            _password = password;
            _endpoint = (IPEndPoint) _udp.Client.LocalEndPoint;
            _startTime = DateTimeOffset.Now;
            _active = false;
        }

        public void Start()
        {
            if (_active)
                throw new InvalidOperationException("Already active");

            _active = true;

            _task = Task.Run(async () =>
            {
                while (_active)
                {
                    var datagram = await _udp.ReceiveAsync().ConfigureAwait(false);
                    var data = datagram.Buffer;
                    var endpoint = datagram.RemoteEndPoint;
                    
                    Console.WriteLine("got packet");

                    if (data.Length <= 2)
                    {
                        if (data[0] != (byte) MessageIdentifiers.OpenConnectionRequest) continue;

                        if (!_connections.ContainsKey(endpoint))
                            _connections[endpoint] = new ReliabilityLayer(_udp, endpoint);

                        var conn = _connections[endpoint];

                        if (!conn.Active)
                            conn.StartSendLoop();

                        var pkt = new byte[] {(byte) MessageIdentifiers.OpenConnectionReply, 0};

                        await _udp.SendAsync(pkt, pkt.Length, endpoint).ConfigureAwait(false);
                    }
                    else
                    {
                        if (!_connections.TryGetValue(endpoint, out var layer)) continue;

                        try
                        {
                            foreach (var packet in layer.HandleDatagram(data))
                            {
                                var stream = new BitStream(packet);

                                var id = (MessageIdentifiers) stream.ReadByte();

                                switch (id)
                                {
                                    case MessageIdentifiers.ConnectionRequest:
                                        _handleConnectionRequest(stream, endpoint);
                                        break;
                                    case MessageIdentifiers.InternalPing:
                                        _handleInternalPing(stream, endpoint);
                                        break;
                                    case MessageIdentifiers.NewIncomingConnection:
                                        if (NewConnection != null)
                                            await NewConnection(endpoint).ConfigureAwait(false);
                                        break;
                                    case MessageIdentifiers.DisconnectionNotification:
                                        _connections[endpoint].StopSendLoop();
                                        _connections.Remove(endpoint);
                                        
                                        if (Disconnection != null)
                                            await Disconnection(endpoint).ConfigureAwait(false);
                                        break;
                                    case MessageIdentifiers.UserPacketEnum:
                                        if (PacketReceived != null)
                                            await PacketReceived(data, endpoint).ConfigureAwait(false);
                                        break;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }
                }
            });
        }

        public void Send(BitStream stream, IPEndPoint endpoint,
            PacketReliability reliability = PacketReliability.ReliableOrdered)
            => Send(stream, new[] {endpoint}, false, reliability);

        public void Send(BitStream stream, IPEndPoint[] endpoints = null, bool broadcast = false,
            PacketReliability reliability = PacketReliability.ReliableOrdered)
            => Send(stream.BaseBuffer, endpoints, broadcast, reliability);

        public void Send(byte[] data, IPEndPoint endpoint,
            PacketReliability reliability = PacketReliability.ReliableOrdered)
            => Send(data, new[] {endpoint}, false, reliability);

        public void Send(byte[] data, IPEndPoint[] endpoints = null, bool broadcast = false,
            PacketReliability reliability = PacketReliability.ReliableOrdered)
        {
            var recipients = broadcast || endpoints == null ? _connections.Keys.ToArray() : endpoints;

            foreach (var endpoint in recipients)
            {
                if (!_connections.ContainsKey(endpoint))
                    continue;

                _connections[endpoint].Send(data, reliability);
            }
        }

        private void _handleConnectionRequest(BitStream stream, IPEndPoint endpoint)
        {
            var password = stream.ReadBits(stream.BitCount - stream.ReadPosition);
            
            if (password.SequenceEqual(_password))
            {
                var res = new BitStream();

                res.WriteByte((byte) MessageIdentifiers.ConnectionRequestAccepted);
                res.Write(endpoint.Address.GetAddressBytes());
                res.WriteUShort((ushort) endpoint.Port);
                res.Write(new byte[2]);
                res.Write(_endpoint.Address.GetAddressBytes());
                res.WriteUShort((ushort) _endpoint.Port);

                Send(res, endpoint, PacketReliability.Reliable);
            }
            else
                throw new NotImplementedException();
        }

        private void _handleInternalPing(BitStream stream, IPEndPoint endpoint)
        {
            var time = stream.ReadUInt();

            var pong = new BitStream();

            pong.WriteByte((byte) MessageIdentifiers.ConnectedPong);
            pong.WriteUInt(time);
            pong.WriteUInt((uint) (DateTimeOffset.Now.ToUnixTimeMilliseconds() - _startTime.ToUnixTimeMilliseconds()));

            Send(pong, endpoint);
        }
    }
}