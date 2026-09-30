SELECT order_id, amount, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
FROM payments
WHERE order_id IS NULL AND amount >= 1150;
