WITH ProductStats AS (
    SELECT product_id, SUM(revenue) AS total_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT products.product_name, ProductStats.total_revenue
FROM products
LEFT JOIN ProductStats ON products.id = ProductStats.product_id
WHERE ProductStats.total_revenue > 20000;
