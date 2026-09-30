WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS rn
    FROM stores
    WHERE city = 'Singapore'
)
SELECT *
FROM numbered
WHERE rn <= 1
ORDER BY 2 DESC, city
