SELECT merchant, AVG(amount) AS avg_amount, SUM(amount) AS total_amount FROM transactions GROUP BY merchant HAVING COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) > 3
