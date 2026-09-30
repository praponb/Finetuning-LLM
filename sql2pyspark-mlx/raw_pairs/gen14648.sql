SELECT txn_month, min_amount
FROM (
    SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, MIN(amount) AS min_amount
    FROM transactions
    GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM')
) AS s
WHERE min_amount >= 2700
ORDER BY txn_month DESC
