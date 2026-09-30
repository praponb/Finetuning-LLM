SELECT
    order_items.unit_price,
    order_items.quantity,
    orders.order_date,
    orders.shipping_fee,
    products.product_name,
    products.launch_date,
    TRIM(orders.channel) AS clean_channel
FROM order_items
RIGHT JOIN orders ON orders.id = order_items.order_id
INNER JOIN products ON order_items.product_id = products.id
ORDER BY order_date ASC
