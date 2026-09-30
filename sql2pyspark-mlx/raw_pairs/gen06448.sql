SELECT txn_type, CONCAT(merchant, ' - ', txn_type) AS merchant_txn_type
FROM transactions
WHERE is_flagged = TRUE AND amount > 2900
ORDER BY txn_type
