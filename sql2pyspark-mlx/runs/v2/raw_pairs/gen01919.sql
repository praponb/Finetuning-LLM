WITH product_summary AS (
    SELECT product_id, AVG(unit_price) AS avg_unit_price, MAX(quantity) AS max_quantity
    FROM order_items
    WHERE quantity BETWEEN 5 AND 8
    GROUP BY product_id
)
SELECT
    p.id,
    p.cost,
    ps.avg_unit_price,
    ps.max_quantity
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.max_quantity >= 7.0
ORDER BY max_quantity DESC;
