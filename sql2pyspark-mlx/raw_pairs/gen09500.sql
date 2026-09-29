WITH order_summary AS (
    SELECT order_id, MIN(quantity) min_quantity, AVG(quantity) avg_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.discount,
    os.min_quantity,
    os.avg_quantity
FROM orders o
JOIN order_summary os ON o.id = os.order_id
ORDER BY min_quantity ASC, id ASC
LIMIT 5
