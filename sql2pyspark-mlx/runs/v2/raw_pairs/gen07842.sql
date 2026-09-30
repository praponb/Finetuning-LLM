WITH numbered AS (
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS row_num
    FROM shipments
    WHERE status IN ('RETURNED', 'DELIVERED', 'IN_TRANSIT')
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY 2 ASC, 1 DESC
