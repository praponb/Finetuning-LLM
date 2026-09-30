SELECT orders.channel, COUNT(*) AS order_item_count, SUM(order_items.unit_price) AS total_unit_price
FROM order_items
JOIN orders ON order_items.order_id = orders.id
JOIN products ON order_items.product_id = products.id
WHERE SIZE(products.tags) >= 1
GROUP BY orders.channel
HAVING total_unit_price >= 2400
ORDER BY channel DESC
