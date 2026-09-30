WITH order_items_by_order AS (
    SELECT order_id, SUM(quantity) AS total_quantity, COUNT(quantity) AS quantity_count
    FROM order_items
    WHERE quantity <= 3
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.shipping_fee,
    order_items_by_order.total_quantity,
    order_items_by_order.quantity_count
FROM orders
INNER JOIN order_items_by_order ON orders.id = order_items_by_order.order_id
ORDER BY orders.shipping_fee DESC, order_items_by_order.total_quantity DESC
