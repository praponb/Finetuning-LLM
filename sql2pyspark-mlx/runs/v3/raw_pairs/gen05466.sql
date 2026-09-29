SELECT payments.id, payments.method, payments.order_id
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE channel IN ('web', 'partner', 'mobile'))
ORDER BY 1
