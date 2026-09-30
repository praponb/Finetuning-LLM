SELECT payments.id, payments.method
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee > 30)
