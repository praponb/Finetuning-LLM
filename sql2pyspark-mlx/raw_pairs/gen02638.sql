SELECT a.status, COUNT(*) AS total_count, MAX(t.txn_time) AS last_txn_time FROM transactions t INNER JOIN accounts a ON t.account_id = a.id GROUP BY a.status ORDER BY last_txn_time;
