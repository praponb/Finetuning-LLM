SELECT txn_type, COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count
FROM transactions
WHERE is_flagged
GROUP BY txn_type
ORDER BY txn_type ASC
LIMIT 5;
