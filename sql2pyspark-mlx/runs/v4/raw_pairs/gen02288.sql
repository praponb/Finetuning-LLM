SELECT *
FROM (
    SELECT
        brand,
        category,
        price
    FROM products
) src
PIVOT (
    SUM(price) AS value_sum
    FOR category IN ('Grocery' AS grocery, 'Home' AS home)
);
