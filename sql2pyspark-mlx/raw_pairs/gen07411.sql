SELECT
    order_items.product_id,
    order_items.unit_price,
    products.launch_date,
    products.category,
    orders.order_date,
    orders.store_id
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
INNER JOIN orders ON orders.id = order_items.order_id
WHERE ARRAY_CONTAINS(products.tags, 'sale') AND order_items.order_id = 16;
