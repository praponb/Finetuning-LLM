WITH product_summary AS (
    SELECT product_id, SUM(revenue) AS total_revenue, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    GROUP BY product_id
)
SELECT
    products.brand,
    products.supplier_id,
    product_summary.total_revenue,
    product_summary.revenue_range
FROM products
LEFT JOIN product_summary ON products.id = product_summary.product_id
WHERE product_summary.total_revenue >= 13000
ORDER BY product_summary.revenue_range DESC;
