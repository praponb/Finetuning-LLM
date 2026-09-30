SELECT id, txn_type, account_id
FROM transactions
WHERE HOUR(txn_time) BETWEEN 8 AND 16
INTERSECT
SELECT id, txn_type, account_id
FROM transactions
WHERE merchant IN ('Starbucks', 'Amazon')
ORDER BY id DESC;
