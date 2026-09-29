WITH order_stats AS (
    SELECT order_id, AVG(quantity) avg_quantity, MAX(unit_price) max_unit_price
    FROM order_items
    WHERE quantity BETWEEN 5 AND 6
    GROUP BY order_id
)
SELECT
    o.id,
    o.amount,
    os.avg_quantity,
    os.max_unit_price
FROM orders o
INNER JOIN order_stats os ON o.id = os.order_id
WHERE os.max_unit_price > 310
ORDER BY avg_quantity, os.max_unit_price DESC
