SELECT
    id,
    account_id,
    txn_time,
    amount,
    LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_amount
FROM transactions
WHERE account_id IN (9, 14, 30);
