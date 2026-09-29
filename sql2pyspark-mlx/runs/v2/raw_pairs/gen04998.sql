SELECT
    id,
    paid_at,
    amount,
    LEAD(amount, 1) OVER (ORDER BY paid_at, id) AS next_amount
FROM payments
WHERE order_id IN (4, 11, 17, 28)
ORDER BY 3;
