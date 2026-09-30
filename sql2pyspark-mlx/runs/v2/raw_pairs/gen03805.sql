SELECT o.channel, SUM(CASE WHEN s.status = 'RETURNED' THEN s.cost ELSE 0 END) AS returned_cost
FROM shipments s
JOIN orders o ON o.id = s.order_id
JOIN customers c ON o.customer_id = c.id
GROUP BY o.channel
ORDER BY o.channel, returned_cost;
