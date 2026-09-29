WITH order_items_by_order AS (
    SELECT order_id, SUM(quantity) AS total_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT o.store_id, oibo.total_quantity
FROM orders o
LEFT JOIN order_items_by_order oibo ON o.id = oibo.order_id
WHERE oibo.total_quantity >= 20
ORDER BY store_id;
