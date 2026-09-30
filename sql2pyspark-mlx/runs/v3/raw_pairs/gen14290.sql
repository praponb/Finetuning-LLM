SELECT
    DATE_FORMAT(txn_time, 'yyyy-MM') txn_month,
    merchant,
    MAX(txn_time) last_txn_time,
    AVG(amount) avg_amount
FROM transactions
WHERE merchant = 'Grab'
GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM'), merchant
ORDER BY avg_amount ASC
