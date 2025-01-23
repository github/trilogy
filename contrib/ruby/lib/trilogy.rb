# frozen_string_literal: true

require "trilogy/version"
require "trilogy/error"
require "trilogy/result"
require "trilogy/cext"
require "trilogy/encoding"

class Trilogy
  def initialize(options = {})
    options[:port] = options[:port].to_i if options[:port]
    mysql_encoding = options[:encoding] || "utf8mb4"
    encoding = Trilogy::Encoding.find(mysql_encoding)
    charset = Trilogy::Encoding.charset(mysql_encoding)
    @connection_options = options
    @connected_host = nil

    if (host = options[:host]) || (path = options[:socket])
      begin
        if host
          port = options[:port] || 3306
          @socket = TCPSocket.new(host, port, connect_timeout: 1)

          # TODO: this probably needs to set keepalive and similar options
        else
          @socket = UNIXSocket.new(path)
        end
      rescue IO::TimeoutError => e
        raise Trilogy::TimeoutError, e.message
      rescue Socket::ResolutionError => e
        connection_str = host ? "#{host}:#{port}" : path
        raise Trilogy::BaseConnectionError, "unable to connect to \"#{connection_str}\": #{e.message}"
      rescue => e
        if e.respond_to?(:errno)
          raise Trilogy::SyscallError.from_errno(e.errno, e.message)
        else
          raise
        end
      end
      @socket.autoclose = false
      options[:raw_socket] = @socket
    end

    _connect(encoding, charset, options)
  end

  def connection_options
    @connection_options.dup.freeze
  end

  def in_transaction?
    (server_status & SERVER_STATUS_IN_TRANS) != 0
  end

  def server_info
    version_str = server_version

    if /\A(\d+)\.(\d+)\.(\d+)/ =~ version_str
      version_num = ($1.to_i * 10000) + ($2.to_i * 100) + $3.to_i
    end

    { :version => version_str, :id => version_num }
  end

  def connected_host
    @connected_host ||= query_with_flags("select @@hostname", query_flags | QUERY_FLAGS_FLATTEN_ROWS).rows.first
  end

  def query_with_flags(sql, flags)
    old_flags = query_flags
    self.query_flags = flags

    query(sql)
  ensure
    self.query_flags = old_flags
  end
end
