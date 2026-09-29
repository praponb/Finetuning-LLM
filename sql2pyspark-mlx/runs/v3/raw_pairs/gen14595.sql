SELECT HOUR(txn_time) AS txn_hour, AVG(amount) AS avg_amount, COUNT(*) AS cnt FROM transactions WHERE account_id = 19 GROUP BY HOUR(txn_time) ORDER BY avg_amount DESC;
