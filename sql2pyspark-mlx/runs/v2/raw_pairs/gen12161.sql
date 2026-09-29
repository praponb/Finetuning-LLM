SELECT
    id,
    status,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share_2
FROM payments
WHERE paid_at < '2024-11-01';
