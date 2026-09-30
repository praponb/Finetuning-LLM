SELECT merchant, txn_type
FROM transactions
WHERE amount <= 1300
UNION
SELECT merchant, txn_type
FROM transactions
WHERE amount BETWEEN 1100 AND 1600
ORDER BY txn_type
