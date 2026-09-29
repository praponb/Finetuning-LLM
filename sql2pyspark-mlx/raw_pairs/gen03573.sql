WITH numbered AS (
    SELECT
        id,
        category,
        cost,
        price,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY cost DESC) AS row_num
    FROM products
)
SELECT *
FROM numbered
WHERE row_num <= 5;
