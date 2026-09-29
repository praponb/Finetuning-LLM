SELECT
    id,
    delivered_date,
    order_id,
    cost
FROM shipments
WHERE delivered_date = (SELECT MAX(delivered_date) FROM shipments)
ORDER BY id
LIMIT 1
