SELECT HOUR(txn_time) AS txn_hour, MAX(amount) - MIN(amount) AS amount_range, COUNT(*) AS num_transactions FROM transactions GROUP BY HOUR(txn_time) ORDER BY amount_range
