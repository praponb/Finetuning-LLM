SELECT id, method, MONTH(paid_at) AS paid_at_month
FROM payments
WHERE status != 'REFUNDED';
