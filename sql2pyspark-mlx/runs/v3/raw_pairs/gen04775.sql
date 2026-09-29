WITH ranked AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS rn
    FROM shipments
)
SELECT
    id,
    carrier,
    shipped_date,
    delivered_date
FROM ranked
WHERE rn = 1
ORDER BY 2 DESC;
