SELECT
    id,
    account_id,
    txn_type,
    amount,
    RANK() OVER (PARTITION BY account_id, txn_type ORDER BY amount) AS rnk
FROM transactions
WHERE txn_time < '2024-10-01'
