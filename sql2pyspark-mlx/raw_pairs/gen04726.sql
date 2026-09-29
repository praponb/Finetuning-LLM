SELECT
    id,
    carrier,
    status,
    cost,
    COUNT(*) OVER (PARTITION BY carrier) AS carrier_count,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY cost) AS dense_rnk
FROM shipments
