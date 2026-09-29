SELECT orders.id, orders.status
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND TO_DATE(payments.paid_at) < '2023-11-25')
ORDER BY id DESC
LIMIT 10
