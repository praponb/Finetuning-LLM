SELECT
    id,
    txn_type,
    amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 3) AS amount_share
FROM transactions;
