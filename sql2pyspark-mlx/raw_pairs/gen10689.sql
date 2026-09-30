WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS pos
    FROM stores
)
SELECT *
FROM ranked
WHERE pos = 1
ORDER BY city DESC, id
