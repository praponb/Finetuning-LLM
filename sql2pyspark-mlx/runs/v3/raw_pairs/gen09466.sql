SELECT
    id,
    merchant,
    account_id,
    txn_time,
    amount,
    LEAD(amount, 2) OVER (PARTITION BY merchant, account_id ORDER BY txn_time, id) AS next_amount
FROM transactions
WHERE account_id IN (5, 8, 26)
