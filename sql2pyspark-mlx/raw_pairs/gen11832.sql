SELECT TO_DATE(txn_time) AS txn_day, COUNT(DISTINCT amount) AS unique_merchants, ROUND(AVG(amount), 2) AS avg_amount FROM transactions GROUP BY TO_DATE(txn_time) HAVING ROUND(AVG(amount), 2) < 76
