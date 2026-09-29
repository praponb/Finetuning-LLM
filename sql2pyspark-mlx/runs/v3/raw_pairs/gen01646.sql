SELECT
    id,
    merchant,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 3) AS amount_share
FROM transactions
WHERE account_id IN (7, 8, 16, 21);
