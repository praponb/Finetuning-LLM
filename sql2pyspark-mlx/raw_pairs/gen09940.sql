SELECT
    order_items.id,
    order_items.unit_price,
    order_items.product_id,
    products.launch_date,
    orders.shipping_fee
FROM order_items
INNER JOIN products ON products.id = order_items.product_id
INNER JOIN orders ON order_items.order_id = orders.id
WHERE products.cost <= 550;
