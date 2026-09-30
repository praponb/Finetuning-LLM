WITH product_stats AS (
    SELECT product_id, SUM(units) AS total_units
    FROM store_sales
    WHERE units BETWEEN 14 AND 33
    GROUP BY product_id
)
SELECT p.price, ps.total_units
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.total_units >= 130
ORDER BY ps.total_units DESC
