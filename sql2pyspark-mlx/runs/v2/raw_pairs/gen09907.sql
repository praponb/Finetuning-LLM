SELECT
    id,
    order_id,
    status,
    cost,
    DAY(shipped_date) AS shipped_date_day
FROM shipments
WHERE status IN ('IN_TRANSIT', 'DELIVERED') AND order_id = 36
ORDER BY shipped_date_day DESC, id DESC
