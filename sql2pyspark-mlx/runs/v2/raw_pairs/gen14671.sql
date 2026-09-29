WITH order_items_by_order AS (
    SELECT order_id, AVG(unit_price) AS avg_unit_price
    FROM order_items
    WHERE order_id IN (9, 19)
    GROUP BY order_id
)
SELECT o.status, o.discount, oibo.avg_unit_price
FROM orders o
LEFT JOIN order_items_by_order oibo ON o.id = oibo.order_id
WHERE oibo.avg_unit_price > 110;
