SELECT
    id,
    method,
    amount,
    SUM(amount) OVER (PARTITION BY method) AS method_sum_amount,
    COUNT(*) OVER (PARTITION BY method) AS method_count
FROM payments
ORDER BY method_sum_amount
