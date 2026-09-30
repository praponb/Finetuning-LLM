WITH top_products AS (
    SELECT
        id,
        category,
        cost,
        brand,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY cost) row_num
    FROM products
)
SELECT
    id,
    category,
    cost,
    brand
FROM top_products
WHERE row_num <= 1
