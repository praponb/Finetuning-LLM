SELECT
    id,
    cost,
    status,
    LAST_DAY(shipped_date) shipped_date_month_end
FROM shipments
WHERE delivered_date >= '2023-07-01' AND order_id IS NOT NULL
ORDER BY status
