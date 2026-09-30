WITH product_summary AS (
    SELECT product_id, COUNT(units) AS units_count
    FROM store_sales
    GROUP BY product_id
)
SELECT products.id, products.product_name, product_summary.units_count
FROM products
JOIN product_summary ON products.id = product_summary.product_id;
