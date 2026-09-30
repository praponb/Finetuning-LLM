WITH product_stats AS (
    SELECT product_id, ROUND(AVG(revenue), 2) AS avg_revenue, COUNT(*) AS cnt
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    ps.avg_revenue,
    ps.cnt
FROM products p
LEFT JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.cnt >= 6;
