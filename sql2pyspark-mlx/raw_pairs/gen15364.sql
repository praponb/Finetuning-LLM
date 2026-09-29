SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount FROM transactions WHERE TO_DATE(txn_time) >= '2023-06-01' GROUP BY 1 HAVING ROUND(AVG(amount), 2) >= 23 ORDER BY avg_amount DESC;
