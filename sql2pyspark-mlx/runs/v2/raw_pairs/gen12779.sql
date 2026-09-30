SELECT
    id,
    paid_at,
    order_id,
    amount,
    CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded
FROM payments
WHERE method = 'bank_transfer'
