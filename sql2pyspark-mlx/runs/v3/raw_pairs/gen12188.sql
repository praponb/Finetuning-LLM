SELECT txn_type, COUNT(DISTINCT amount) AS unique_merchants
FROM transactions
GROUP BY 1
HAVING COUNT(DISTINCT amount) >= 1
ORDER BY unique_merchants, txn_type
LIMIT 3;
