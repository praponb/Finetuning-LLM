SELECT
    id,
    status,
    paid_at,
    amount,
    method,
    LEAD(amount, 1, 0) OVER (PARTITION BY status ORDER BY paid_at, id) AS next_amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY method), 3) AS amount_share
FROM payments
WHERE TO_DATE(paid_at) >= '2025-03-27'
ORDER BY next_amount;
