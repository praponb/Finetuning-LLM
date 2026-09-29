SELECT channel, customer_id, MAX(order_date) AS latest_order_date
FROM orders
GROUP BY channel, customer_id
ORDER BY latest_order_date DESC, customer_id DESC
