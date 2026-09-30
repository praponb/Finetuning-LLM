SELECT
    id,
    cost,
    shipped_date,
    carrier
FROM shipments
WHERE status != 'RETURNED'
