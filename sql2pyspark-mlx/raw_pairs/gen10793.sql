SELECT *, HOUR(paid_at) AS paid_at_hour, ROUND(amount, 2) AS amount_rounded
FROM payments
WHERE order_id IS NULL
