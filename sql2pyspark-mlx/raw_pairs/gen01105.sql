SELECT *
FROM (
    SELECT
        id,
        category,
        price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) row_num
    FROM products
) r
WHERE row_num <= 3
ORDER BY category
