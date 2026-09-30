SELECT
    MONTH(txn_time) AS txn_month,
    merchant,
    AVG(amount) AS avg_amount,
    MIN(txn_time) AS min_txn_time
FROM transactions
WHERE txn_time >= '2025-07-01'
GROUP BY MONTH(txn_time), merchant
HAVING AVG(amount) > 3700
