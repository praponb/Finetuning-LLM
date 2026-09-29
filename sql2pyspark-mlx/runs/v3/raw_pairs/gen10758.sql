SELECT o.customer_id, o.channel, MAX(oi.quantity) AS max_quantity
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.order_date < '2025-03-23'
GROUP BY o.customer_id, o.channel
ORDER BY customer_id DESC;
