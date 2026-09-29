SELECT id, channel, COUNT(*) OVER (PARTITION BY channel) AS channel_count FROM orders WHERE channel IN ('mobile', 'store', 'partner') ORDER BY id
