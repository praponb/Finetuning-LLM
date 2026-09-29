SELECT
    id,
    account_id,
    txn_time,
    amount,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
FROM transactions;
