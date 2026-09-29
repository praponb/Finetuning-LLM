SELECT
    payments.id,
    payments.method,
    payments.status,
    payments.amount
FROM payments
WHERE payments.order_id NOT IN (SELECT id FROM orders WHERE store_id = 8);
