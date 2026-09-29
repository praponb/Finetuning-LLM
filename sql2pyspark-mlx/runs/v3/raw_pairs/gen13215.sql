SELECT id, discount, COALESCE(channel, 'Unknown') AS channel_filled
FROM orders
WHERE discount > 0.1
ORDER BY id DESC;
