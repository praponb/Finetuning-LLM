WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY sqft) AS row_num
    FROM stores
)
SELECT *
FROM numbered
WHERE row_num <= 1
