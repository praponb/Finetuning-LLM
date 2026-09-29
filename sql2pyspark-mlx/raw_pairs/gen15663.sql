WITH product_summary AS (
    SELECT product_id, SUM(units) total_units, ROUND(AVG(revenue), 2) avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.brand,
    p.price,
    ps.total_units,
    ps.avg_revenue
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.total_units >= 200;
