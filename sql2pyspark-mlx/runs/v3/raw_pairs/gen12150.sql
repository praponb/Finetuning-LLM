WITH product_stats AS (
    SELECT product_id, MIN(quantity) AS min_quantity, COUNT(*) AS total_count
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    ps.min_quantity,
    ps.total_count
FROM products p
INNER JOIN product_stats ps ON p.id = ps.product_id;
