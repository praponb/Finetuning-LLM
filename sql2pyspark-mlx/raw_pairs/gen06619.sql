WITH product_summary AS (
    SELECT product_id, SUM(units) total_units
    FROM store_sales
    GROUP BY product_id
)
SELECT p.category, ps.total_units
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
ORDER BY category
