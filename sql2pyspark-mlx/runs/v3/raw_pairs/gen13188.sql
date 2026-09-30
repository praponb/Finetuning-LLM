SELECT channel, status, AVG(discount) AS avg_discount
FROM orders
WHERE status NOT IN ('SHIPPED', 'PENDING')
GROUP BY ROLLUP(channel, status)
ORDER BY channel;
