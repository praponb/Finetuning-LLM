WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS('/', product_name, category) AS product_name_category_key
    FROM products
    WHERE brand IS NOT NULL
      AND price BETWEEN 920 AND 980
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        price
    FROM filtered
) src
PIVOT (
    SUM(price)
    FOR category IN ('Sports' AS sports, 'Books' AS books, 'Home' AS home)
);
