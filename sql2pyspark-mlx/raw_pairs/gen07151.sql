SELECT s.id, s.cost, s.order_id
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE store_id IS NULL)
ORDER BY s.cost;
