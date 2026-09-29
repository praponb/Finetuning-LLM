WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        tags,
        LENGTH(category) AS category_length
    FROM products
    WHERE launch_date >= DATE '2023-08-06'
)
SELECT brand, id
FROM filtered
EXCEPT
SELECT brand, id
FROM products
WHERE launch_date <= '2024-09-30';
