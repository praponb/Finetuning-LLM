SELECT
    id,
    merchant,
    account_id,
    txn_time,
    PERCENT_RANK() OVER (PARTITION BY merchant, account_id ORDER BY txn_time DESC) AS txn_time_pct_rank
FROM transactions;
