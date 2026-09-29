SELECT
    id,
    amount,
    status,
    FLOOR(amount) AS amount_whole
FROM payments
WHERE amount BETWEEN 580 AND 1050 AND paid_at < '2024-07-29' AND status <> 'FAILED'
ORDER BY id DESC
