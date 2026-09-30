SELECT s.id, s.status, o.store_id
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
WHERE o.customer_id = 32
ORDER BY o.store_id;
