WITH product_summary AS (
    SELECT product_id, MAX(units) AS max_units, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT p.category, ps.max_units, ps.avg_revenue
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.avg_revenue >= 41
