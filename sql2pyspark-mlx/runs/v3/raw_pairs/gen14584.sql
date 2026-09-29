SELECT o.amount, t.cost_count FROM orders o INNER JOIN (SELECT order_id, COUNT(cost) AS cost_count FROM shipments GROUP BY order_id) t ON o.id = t.order_id
