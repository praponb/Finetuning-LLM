SELECT
    order_items.quantity,
    order_items.order_id,
    order_items.unit_price,
    orders.store_id,
    orders.channel,
    CAST(orders.discount AS INT) AS discount_int
FROM order_items
LEFT JOIN orders ON order_items.order_id = orders.id;
