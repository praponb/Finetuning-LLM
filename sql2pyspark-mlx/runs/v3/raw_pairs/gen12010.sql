WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS rnk
    FROM stores
)
SELECT *
FROM numbered
WHERE rnk <= 2
