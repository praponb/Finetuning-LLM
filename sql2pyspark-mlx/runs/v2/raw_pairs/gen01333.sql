WITH product_stats AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_stores, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    WHERE product_id = 9
    GROUP BY product_id
)
SELECT
    p.id,
    p.price,
    ps.unique_stores,
    ps.avg_quantity
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id
ORDER BY price DESC;
