SELECT orders.store_id, ROUND(STDDEV(order_items.quantity), 2) AS stddev_quantity, COUNT(DISTINCT order_items.quantity) AS unique_products
FROM order_items
INNER JOIN orders ON orders.id = order_items.order_id
GROUP BY orders.store_id;
