SELECT
    id,
    method,
    amount,
    COUNT(*) OVER (PARTITION BY method) AS method_count,
    MAX(amount) OVER (PARTITION BY method) AS method_max_amount
FROM payments
WHERE status = 'SUCCESS';
