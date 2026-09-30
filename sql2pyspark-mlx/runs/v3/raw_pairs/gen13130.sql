SELECT
    orders.id,
    orders.amount,
    orders.channel,
    t.unit_price_count
FROM orders
LEFT JOIN (SELECT order_id, COUNT(unit_price) unit_price_count
    FROM order_items
    GROUP BY order_id) t ON orders.id = t.order_id
WHERE t.unit_price_count > 5
ORDER BY orders.channel ASC
