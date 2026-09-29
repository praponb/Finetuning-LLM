SELECT
    id,
    account_id,
    txn_time,
    merchant,
    amount,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS txn_time_row,
    LEAD(amount, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS next_amount
FROM transactions
ORDER BY txn_time_row DESC
