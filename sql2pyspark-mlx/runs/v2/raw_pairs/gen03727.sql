SELECT id, channel, SUBSTRING(status, 1, 2) AS status_prefix FROM orders ORDER BY id DESC, channel;
