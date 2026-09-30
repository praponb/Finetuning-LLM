WITH product_stats AS (
    SELECT product_id, COUNT(quantity) AS quantity_count, MIN(unit_price) AS min_unit_price
    FROM order_items
    GROUP BY product_id
)
SELECT
    products.id,
    products.price,
    product_stats.quantity_count,
    product_stats.min_unit_price
FROM products
INNER JOIN product_stats ON products.id = product_stats.product_id
ORDER BY 1 ASC
LIMIT 3
