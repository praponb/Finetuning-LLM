SELECT
    id,
    shipped_date,
    status,
    delivered_date
FROM shipments
WHERE shipped_date = (SELECT MIN(shipped_date) FROM shipments WHERE shipped_date >= '2024-01-01')
