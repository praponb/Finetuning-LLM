SELECT s.id, s.shipped_date, s.cost
FROM shipments s
WHERE s.order_id IN (SELECT id FROM orders WHERE customer_id IS NULL)
