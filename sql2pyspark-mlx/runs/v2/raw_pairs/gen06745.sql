WITH ranked AS (
    SELECT
        id,
        status,
        shipped_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS rnk
    FROM shipments
    WHERE MONTH(delivered_date) IN (7, 9, 10)
)
SELECT id, status, shipped_date
FROM ranked
WHERE rnk <= 5;
