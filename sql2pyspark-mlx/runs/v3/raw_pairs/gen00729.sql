SELECT
    order_items.id,
    order_items.quantity,
    order_items.unit_price,
    orders.discount
FROM order_items
JOIN orders ON order_items.order_id = orders.id
ORDER BY quantity DESC;
