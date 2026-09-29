WITH order_items_by_order AS (
    SELECT order_id, COUNT(unit_price) unit_price_count
    FROM order_items
    GROUP BY order_id
)
SELECT o.discount, o.channel, oibo.unit_price_count
FROM orders o
LEFT JOIN order_items_by_order oibo ON o.id = oibo.order_id
