SELECT s.cost, o.store_id
FROM shipments s
JOIN orders o ON s.order_id = o.id
ORDER BY s.cost DESC;
