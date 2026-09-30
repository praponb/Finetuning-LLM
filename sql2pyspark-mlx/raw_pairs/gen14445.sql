SELECT s.cost, o.customer_id
FROM shipments s
JOIN orders o ON o.id = s.order_id
WHERE o.store_id IN (11, 19)
ORDER BY customer_id;
