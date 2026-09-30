SELECT
    id,
    account_id,
    txn_time,
    DENSE_RANK() OVER (PARTITION BY account_id ORDER BY txn_time) txn_time_dense_rank
FROM transactions
WHERE merchant IN ('Grab', 'Amazon');
