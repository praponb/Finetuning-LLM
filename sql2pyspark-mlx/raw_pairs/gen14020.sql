SELECT txn_type, txn_month, avg_amount
FROM (
    SELECT txn_type, MONTH(txn_time) AS txn_month, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    WHERE is_flagged = TRUE
    GROUP BY txn_type, MONTH(txn_time)
) AS x
WHERE avg_amount < 23
