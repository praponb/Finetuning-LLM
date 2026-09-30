SELECT oi.quantity, o.channel, o.customer_id
FROM order_items oi
LEFT OUTER JOIN orders o ON oi.order_id = o.id
WHERE oi.unit_price BETWEEN 200 AND 380 OR oi.product_id IN (1, 6, 25, 30)
