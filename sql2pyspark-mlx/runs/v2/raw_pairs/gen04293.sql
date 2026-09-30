SELECT
    order_items.id,
    order_items.product_id,
    order_items.unit_price,
    orders.order_date,
    products.brand,
    products.launch_date
FROM order_items
LEFT JOIN orders ON orders.id = order_items.order_id
INNER JOIN products ON order_items.product_id = products.id;
