SELECT
    id,
    account_id,
    txn_time,
    amount,
    amount - LAG(amount, 1, 0) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS amount_change
FROM transactions
WHERE amount < 1000;
