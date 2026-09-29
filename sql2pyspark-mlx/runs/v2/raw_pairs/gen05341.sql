SELECT
    id,
    status,
    delivered_date,
    NTILE(2) OVER (PARTITION BY status ORDER BY delivered_date DESC, id) quartile
FROM shipments;
