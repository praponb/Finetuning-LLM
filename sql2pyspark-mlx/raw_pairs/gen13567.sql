SELECT s.carrier, o.store_id
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
WHERE o.customer_id IN (7, 20, 26, 33);
