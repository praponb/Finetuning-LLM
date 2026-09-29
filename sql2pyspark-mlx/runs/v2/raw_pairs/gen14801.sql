WITH product_summary AS (
    SELECT product_id, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT p.cost, p.category, ps.avg_revenue
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
ORDER BY avg_revenue, p.cost;
