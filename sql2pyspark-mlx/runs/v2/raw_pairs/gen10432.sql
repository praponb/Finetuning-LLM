SELECT
    id,
    status,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share
FROM orders
WHERE discount BETWEEN 0.11 AND 0.18
