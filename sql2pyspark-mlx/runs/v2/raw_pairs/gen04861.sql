SELECT payments.id, payments.method
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE discount < 0.13);
