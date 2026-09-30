SELECT channel, status, SUM(discount) AS total_discount
FROM orders
GROUP BY channel, status
ORDER BY status DESC, channel
LIMIT 20;
