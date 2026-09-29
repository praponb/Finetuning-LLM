SELECT
    s.id,
    s.order_id,
    s.shipped_date,
    s.cost
FROM shipments s
WHERE s.shipped_date > '2024-04-01' AND s.order_id NOT IN (SELECT id FROM orders WHERE store_id IS NULL)
