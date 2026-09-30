SELECT
    id,
    status,
    delivered_date,
    NTILE(3) OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS bucket
FROM shipments
ORDER BY status DESC
