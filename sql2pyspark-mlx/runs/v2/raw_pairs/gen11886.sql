WITH product_summary AS (
    SELECT product_id, ROUND(AVG(units), 2) AS avg_units
    FROM store_sales
    WHERE store_id = 2
    GROUP BY product_id
)
SELECT p.id, p.cost, ps.avg_units
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.avg_units >= 64
ORDER BY p.id;
