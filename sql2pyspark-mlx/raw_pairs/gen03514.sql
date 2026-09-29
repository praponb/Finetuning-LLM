SELECT orders.id, orders.shipping_fee, orders.customer_id
FROM orders
WHERE YEAR(orders.order_date) = 2025 AND EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id AND payments.amount BETWEEN 550 AND 1420)
