SELECT
    id,
    account_id,
    txn_time,
    txn_type,
    DENSE_RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS dense_rnk,
    PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY txn_time DESC) AS txn_time_pct_rank
FROM transactions;
