SELECT DISTINCT channel, status
FROM orders
ORDER BY status, channel
LIMIT 5;
