SELECT MONTH(txn_time) AS txn_month, SUM(amount) AS total_amount
FROM transactions
GROUP BY MONTH(txn_time)
HAVING total_amount < 21000
