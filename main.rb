require 'trollop'
require 'aws-sdk'
require 'net/http'
require 'uri'

class Main
	def run
		@opts = Trollop::options do 
			opt :worker_id, "me", :type => :string
			opt :aws_access_key_id, "aws access key", :default => ENV["AWS_ACCESS_KEY_ID"]
			opt :aws_secret_access_key, "aws secret access key", :default => ENV["AWS_SECRET_ACCESS_KEY"]
			opt :bucket_names_queue, "read bucket names from this queue", :default => "names"
			opt :bucket_status_queue, "write bucket status to this queue", :default => "status" 
			opt :aws_host, "host to make s3 requests against", :default => 's3.amazonaws.com'
		end

		@sqs = AWS::SQS.new :access_key_id => opts[:aws_access_key_id],
							:secret_access_key => opts[:aws_secret_access_key]

		@bucket_names_queue = queue_named @opts[:bucket_names_queue]
		@bucket_status_queue = queue_named @opts[:bucket_status_queue]

		@bucket_names_queue.poll do |msg|
			process_message msg
		end
	end

	def check_status(bucket_name)
		uri = URI.parse "http://#{@opts[:aws_host]}/#{bucket_name}"
		Net::HTTP.get_response(uri).code
	end


	def process_message(msg)
		obj = YAML.load msg.body
		now = Time.now
		status = check_status obj[:bucket_name]
		time = Time.now - now
		obj.merge! 	:bucket_status => status,
					:processed_by => @opts[:worker_id],
					:request_time => time
		@bucket_status_queue.send_message obj.to_yaml
	end

	def queue_named(name)
		begin 
			@sqs.queues.named name
		rescue 
			@sqs.queues.create name
		end
	end
end
