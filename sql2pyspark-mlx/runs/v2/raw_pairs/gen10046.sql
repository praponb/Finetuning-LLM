WITH product_stats AS (
    SELECT product_id, COUNT(units) AS units_count
    FROM store_sales
    GROUP BY product_id
)
SELECT products.brand, product_stats.units_count
FROM products
JOIN product_stats ON products.id = product_stats.product_id;
