SELECT
    id,
    txn_time,
    account_id,
    merchant,
    amount,
    RANK() OVER (PARTITION BY MONTH(txn_time), account_id ORDER BY txn_time) AS txn_time_rank,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 3) AS amount_share
FROM transactions
WHERE TO_DATE(txn_time) < '2023-10-01';
