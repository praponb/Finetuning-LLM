SELECT
    order_items.id,
    order_items.quantity,
    order_items.product_id,
    order_items.unit_price,
    orders.customer_id
FROM order_items
INNER JOIN orders ON orders.id = order_items.order_id
