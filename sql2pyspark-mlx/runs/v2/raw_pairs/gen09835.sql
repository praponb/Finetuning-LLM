WITH order_items_by_order AS (
    SELECT order_id, MAX(quantity) AS max_quantity
    FROM order_items
    GROUP BY order_id
)
SELECT o.store_id, o.status, oibo.max_quantity
FROM orders o
LEFT JOIN order_items_by_order oibo ON o.id = oibo.order_id
ORDER BY 3 DESC, status
