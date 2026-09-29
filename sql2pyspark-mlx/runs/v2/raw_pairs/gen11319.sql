SELECT
    id,
    account_id,
    txn_time,
    RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS txn_time_rank
FROM transactions
WHERE NOT is_flagged
