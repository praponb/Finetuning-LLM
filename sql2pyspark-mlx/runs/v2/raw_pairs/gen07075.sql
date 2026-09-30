SELECT
    id,
    status,
    shipped_date,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY shipped_date DESC) AS dense_rnk
FROM shipments;
