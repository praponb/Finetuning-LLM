WITH top_products AS (
    SELECT
        id,
        brand,
        price,
        cost,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price, id) row_num
    FROM products
)
SELECT
    id,
    brand,
    price,
    cost
FROM top_products
WHERE row_num = 1;
