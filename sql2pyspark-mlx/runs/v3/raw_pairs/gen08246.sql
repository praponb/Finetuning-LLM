SELECT
    id,
    carrier,
    shipped_date,
    RANK() OVER (PARTITION BY carrier ORDER BY shipped_date) shipped_date_rank
FROM shipments
ORDER BY carrier DESC;
