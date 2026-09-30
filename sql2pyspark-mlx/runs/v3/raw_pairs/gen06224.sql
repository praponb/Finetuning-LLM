SELECT txn_time, txn_type, UPPER(txn_type) AS txn_type_upper
FROM transactions
WHERE is_flagged
ORDER BY txn_time DESC;
