SELECT
    id,
    method,
    amount,
    AVG(amount) OVER (PARTITION BY method) AS method_avg_amount
FROM payments
