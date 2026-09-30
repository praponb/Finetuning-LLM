WITH product_stats AS (
    SELECT product_id, COUNT(unit_price) AS unit_price_count
    FROM order_items
    WHERE unit_price >= 400
    GROUP BY product_id
)
SELECT products.brand, products.launch_date, product_stats.unit_price_count
FROM products
LEFT JOIN product_stats ON products.id = product_stats.product_id
