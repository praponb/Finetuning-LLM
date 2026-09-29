WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS pos
    FROM stores
    WHERE region = 'Central'
)
SELECT id, region, opened_date
FROM numbered
WHERE pos <= 2;
