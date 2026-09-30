SELECT a.status, ROUND(AVG(t.amount), 2) AS avg_amount FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id WHERE t.txn_time >= '2024-01-01' GROUP BY a.status;
