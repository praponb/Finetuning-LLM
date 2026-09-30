SELECT
    id,
    txn_type,
    merchant,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type, merchant), 3) AS amount_share
FROM transactions
WHERE txn_type IN ('FEE', 'CREDIT')
ORDER BY txn_type
