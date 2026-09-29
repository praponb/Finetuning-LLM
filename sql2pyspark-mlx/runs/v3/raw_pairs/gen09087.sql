SELECT
    id,
    category,
    brand,
    cost,
    SUBSTRING(product_name, 1, 2) AS product_name_prefix
FROM products
WHERE product_name LIKE '%Lapto%'
ORDER BY category DESC, id
LIMIT 50;
