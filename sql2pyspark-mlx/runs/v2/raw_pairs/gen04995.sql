SELECT
    order_items.id,
    order_items.unit_price,
    order_items.quantity,
    orders.store_id,
    orders.shipping_fee,
    stores.store_name,
    stores.region
FROM order_items
JOIN orders ON order_items.order_id = orders.id
JOIN stores ON orders.store_id = stores.id
WHERE order_items.quantity <> 8
ORDER BY store_id;
