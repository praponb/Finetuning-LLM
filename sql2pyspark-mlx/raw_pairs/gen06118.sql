SELECT DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month, o.channel, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM'), o.channel
ORDER BY order_month DESC, channel
LIMIT 20
