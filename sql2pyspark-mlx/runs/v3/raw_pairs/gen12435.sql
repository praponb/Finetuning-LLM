SELECT
    txn_type,
    SUM(amount) AS total_amount,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE 0 END) AS credit_amount,
    MAX(txn_time) AS latest_txn_time
FROM transactions
WHERE (HOUR(txn_time) BETWEEN 8 AND 15 AND amount >= 1900) OR NOT is_flagged
GROUP BY txn_type
ORDER BY credit_amount DESC, total_amount DESC, txn_type
LIMIT 5;
