SELECT orders.channel, COUNT(payments.id) AS payment_count FROM orders LEFT JOIN payments ON payments.order_id = orders.id GROUP BY orders.channel
