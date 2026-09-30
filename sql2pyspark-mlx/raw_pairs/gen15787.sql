WITH numbered AS (
    SELECT
        id,
        city,
        sqft,
        opened_date,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft) row_num
    FROM stores
)
SELECT
    id,
    city,
    sqft,
    opened_date
FROM numbered
WHERE row_num = 1
ORDER BY opened_date
