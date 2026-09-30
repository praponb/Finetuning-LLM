SELECT
    id,
    merchant,
    txn_time,
    account_id,
    amount,
    PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY txn_time DESC) AS txn_time_pct_rank,
    LEAD(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_amount
FROM transactions
ORDER BY account_id DESC, txn_time_pct_rank;
