SELECT
    id,
    method,
    amount,
    MAX(amount) OVER (PARTITION BY method) AS method_max_amount
FROM payments;
