SELECT
    id,
    account_id,
    txn_time,
    amount,
    merchant,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id) running_amount,
    ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC, id) rn
FROM transactions
ORDER BY 1, account_id
