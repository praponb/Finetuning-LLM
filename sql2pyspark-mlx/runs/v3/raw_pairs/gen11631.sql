SELECT
    id,
    merchant,
    account_id,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant, account_id), 3) AS amount_share
FROM transactions
ORDER BY id
