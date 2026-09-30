SELECT amount, CONCAT(merchant, ', ', txn_type) AS merchant_txn_type, LOWER(txn_type) AS txn_type_lower
FROM transactions
WHERE amount >= 3000 AND txn_type IN ('REFUND', 'CREDIT') AND NOT is_flagged
ORDER BY txn_type_lower;
