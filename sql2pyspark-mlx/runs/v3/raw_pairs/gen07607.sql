SELECT TO_DATE(txn_time) AS txn_day, MAX(amount) - MIN(amount) AS amount_range FROM transactions GROUP BY TO_DATE(txn_time) HAVING SUM(amount) >= 11000
