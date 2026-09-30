WITH product_stats AS (
    SELECT product_id, SUM(quantity) total_quantity, MIN(quantity) min_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.price,
    p.category,
    ps.total_quantity,
    ps.min_quantity
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id;
