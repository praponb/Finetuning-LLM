SELECT status, paid_at
FROM payments
WHERE order_id IS NULL
