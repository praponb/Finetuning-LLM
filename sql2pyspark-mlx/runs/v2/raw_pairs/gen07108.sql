SELECT HOUR(txn_time) AS txn_hour, SUM(amount) AS total_amount
FROM transactions
GROUP BY HOUR(txn_time)
HAVING SUM(amount) > 13000;
