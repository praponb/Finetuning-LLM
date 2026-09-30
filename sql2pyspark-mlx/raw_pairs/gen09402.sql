WITH product_stats AS (
    SELECT product_id, SUM(units) AS total_units
    FROM store_sales
    WHERE revenue <= 3500
    GROUP BY product_id
)
SELECT p.price, ps.total_units
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.total_units >= 170
