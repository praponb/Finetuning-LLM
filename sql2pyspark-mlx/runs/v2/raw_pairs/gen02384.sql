SELECT
    id,
    status,
    amount,
    AVG(amount) OVER (PARTITION BY status) AS status_avg_amount
FROM payments;
