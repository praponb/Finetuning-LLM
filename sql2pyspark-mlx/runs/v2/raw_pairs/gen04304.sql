SELECT payments.id, payments.amount
FROM payments
WHERE TO_DATE(payments.paid_at) >= '2023-07-01' AND payments.order_id IN (SELECT id FROM orders WHERE customer_id IN (14, 19, 26, 36))
