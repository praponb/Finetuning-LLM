WITH product_summary AS (
    SELECT product_id, SUM(revenue) total_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT products.id, products.supplier_id, product_summary.total_revenue
FROM products
INNER JOIN product_summary ON products.id = product_summary.product_id;
