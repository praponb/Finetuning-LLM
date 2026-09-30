SELECT HOUR(txn_time) AS txn_hour, SUM(amount) AS total_amount, MIN(amount) AS min_amount FROM transactions GROUP BY HOUR(txn_time) HAVING MIN(amount) < 1800
