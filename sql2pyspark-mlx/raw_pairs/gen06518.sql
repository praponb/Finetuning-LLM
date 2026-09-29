WITH order_items_by_order AS (
    SELECT order_id, SUM(unit_price * quantity) AS revenue
    FROM order_items
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.shipping_fee,
    orders.status,
    order_items_by_order.revenue
FROM orders
LEFT JOIN order_items_by_order ON orders.id = order_items_by_order.order_id
ORDER BY shipping_fee DESC;
