WITH product_stats AS (
    SELECT product_id, AVG(quantity) AS avg_quantity, COUNT(*) AS cnt
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    ps.avg_quantity,
    ps.cnt
FROM products p
JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.cnt >= 5;
