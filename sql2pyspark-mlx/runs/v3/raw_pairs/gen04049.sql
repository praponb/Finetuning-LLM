SELECT *, CONCAT_WS('-', status, channel) AS status_channel_key, COALESCE(channel, 'unassigned') AS channel_filled FROM orders WHERE amount <= 1560
