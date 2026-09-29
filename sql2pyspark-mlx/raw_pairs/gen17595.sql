SELECT
    id,
    account_id,
    txn_time,
    amount,
    LEAD(amount, 2, 0) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_amount
FROM transactions
WHERE amount BETWEEN 2300 AND 2500
