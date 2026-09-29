SELECT
    id,
    account_id,
    txn_time,
    amount,
    LEAD(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_amount
FROM transactions;
