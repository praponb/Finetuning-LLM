SELECT
    id,
    amount,
    method,
    paid_at,
    COALESCE(amount, 0) AS amount_filled
FROM payments
WHERE method IN ('promptpay', 'card', 'bank_transfer') AND order_id IN (8, 10, 17, 28)
