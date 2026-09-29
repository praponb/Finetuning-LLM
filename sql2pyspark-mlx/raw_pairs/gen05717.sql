SELECT orders.id, orders.status, COUNT(payments.id) payment_count FROM orders LEFT JOIN payments ON orders.id = payments.order_id GROUP BY orders.id, orders.status
