SELECT
    id,
    delivered_date,
    order_id,
    cost
FROM shipments
WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments)
ORDER BY order_id DESC
