SELECT payments.id, payments.amount
FROM payments
WHERE payments.amount BETWEEN 470 AND 900 AND payments.order_id IN (SELECT id FROM orders WHERE channel = 'mobile')
