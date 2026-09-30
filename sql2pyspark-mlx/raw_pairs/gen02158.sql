SELECT *
FROM (
    SELECT
        brand,
        category,
        cost
    FROM products
) src
PIVOT (
    AVG(cost) AS value_sum,
    COUNT(1) AS item_count
    FOR category IN ('Home', 'Books', 'Grocery')
)
