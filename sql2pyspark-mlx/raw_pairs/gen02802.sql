SELECT
    id,
    method,
    amount,
    MIN(amount) OVER (PARTITION BY method) method_min_amount
FROM payments
