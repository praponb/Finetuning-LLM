SELECT
    id,
    account_id,
    txn_time,
    amount,
    LEAD(amount, 2, 0) OVER (PARTITION BY account_id ORDER BY txn_time, id) next_amount
FROM transactions
WHERE amount > 1500
ORDER BY next_amount DESC;
