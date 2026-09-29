SELECT
    id,
    account_id,
    amount,
    txn_time,
    PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount) AS amount_pct_rank,
    PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS txn_time_pct_rank
FROM transactions
