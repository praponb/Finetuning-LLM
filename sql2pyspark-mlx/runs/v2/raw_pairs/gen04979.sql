SELECT
    paid_at,
    amount,
    method,
    DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
FROM payments
WHERE order_id IN (8, 22, 40);
