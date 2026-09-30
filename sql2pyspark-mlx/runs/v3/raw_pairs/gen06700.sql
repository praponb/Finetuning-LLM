SELECT txn_type, MIN(txn_time) first_txn_time, AVG(amount) avg_amount
FROM transactions
WHERE merchant <> 'Starbucks' AND (txn_time >= '2023-03-01' OR merchant IS NOT NULL)
GROUP BY txn_type
HAVING AVG(amount) >= 3100
ORDER BY first_txn_time DESC
