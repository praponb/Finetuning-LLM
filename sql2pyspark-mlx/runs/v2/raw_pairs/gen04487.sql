WITH product_stats AS (
    SELECT product_id, SUM(revenue) AS total_revenue, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT p.category, ps.total_revenue, ps.avg_revenue
FROM products p
LEFT JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.total_revenue > 10000
ORDER BY total_revenue DESC, category;
