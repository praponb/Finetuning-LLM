SELECT
    id,
    merchant,
    txn_time,
    amount,
    account_id,
    SUM(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_amount
FROM transactions
WHERE account_id IN (4, 17)
ORDER BY amount DESC
