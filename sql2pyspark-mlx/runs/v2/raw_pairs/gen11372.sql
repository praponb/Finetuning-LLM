SELECT HOUR(txn_time) AS txn_hour, merchant, COUNT(amount) AS amount_count
FROM transactions
GROUP BY 1, 2
HAVING COUNT(amount) < 3
