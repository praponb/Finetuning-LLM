SELECT
    id,
    status,
    cost,
    shipped_date
FROM (
    SELECT
        id,
        status,
        cost,
        shipped_date,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY cost) AS rnk
    FROM shipments
    WHERE carrier IN ('Kerry', 'DHL')
) AS ranked
WHERE rnk = 1
ORDER BY 3, shipped_date
