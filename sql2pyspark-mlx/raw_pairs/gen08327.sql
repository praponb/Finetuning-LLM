WITH order_summary AS (
    SELECT order_id, AVG(unit_price) avg_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT o.status, os.avg_unit_price
FROM orders o
LEFT OUTER JOIN order_summary os ON o.id = os.order_id
ORDER BY status;
