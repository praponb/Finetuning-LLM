SELECT
    id,
    delivered_date,
    shipped_date,
    status,
    ROUND(DATEDIFF(delivered_date, shipped_date) / 30, 1) months_shipped_date_to_delivered_date
FROM shipments
WHERE delivered_date < '2023-06-01'
ORDER BY months_shipped_date_to_delivered_date DESC, status, id
LIMIT 10
