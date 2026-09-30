SELECT
    id,
    delivered_date,
    status,
    shipped_date
FROM shipments
WHERE delivered_date = (SELECT MIN(delivered_date) FROM shipments WHERE order_id IS NOT NULL)
