SELECT o.channel, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
WHERE st.store_name LIKE 'Do%'
GROUP BY o.channel
ORDER BY o.channel;
