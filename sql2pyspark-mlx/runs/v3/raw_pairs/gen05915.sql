SELECT
    id,
    merchant,
    txn_time,
    amount,
    SUM(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
FROM transactions
WHERE merchant IN ('Amazon', '7-Eleven');
