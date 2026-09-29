SELECT payments.id, payments.amount, payments.paid_at
FROM payments
WHERE payments.order_id IS NULL AND payments.order_id IN (SELECT id FROM orders WHERE store_id IS NOT NULL);
