SELECT orders.status, COUNT(order_items.unit_price) AS unit_price_count, COUNT(order_items.quantity) AS quantity_count
FROM order_items
LEFT OUTER JOIN products ON order_items.product_id = products.id
INNER JOIN orders ON order_items.order_id = orders.id
GROUP BY orders.status
