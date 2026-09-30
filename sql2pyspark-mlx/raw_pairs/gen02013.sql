WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        tags,
        supplier_id,
        LENGTH(product_name) product_name_length
    FROM products
    WHERE supplier_id IS NOT NULL
      AND brand IS NOT DISTINCT FROM 'Globex'
)
SELECT id, category
FROM filtered
INTERSECT
SELECT id, category
FROM products
WHERE launch_date <= '2023-05-01';
