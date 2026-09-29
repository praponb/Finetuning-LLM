SELECT
    id,
    account_id,
    amount,
    txn_time,
    MIN(amount) OVER (PARTITION BY account_id) AS account_id_min_amount,
    RANK() OVER (PARTITION BY account_id ORDER BY txn_time) AS rnk
FROM transactions
