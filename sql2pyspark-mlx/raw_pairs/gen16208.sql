SELECT
    id,
    delivered_date,
    cost,
    shipped_date,
    DATE_TRUNC('YEAR', shipped_date) AS shipped_date_period
FROM shipments
WHERE status = 'DELIVERED' AND status = 'LOST'
