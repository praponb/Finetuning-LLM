SELECT t.id, t.txn_time, a.status, c.city, c.name FROM transactions t INNER JOIN accounts a ON t.account_id = a.id INNER JOIN customers c ON a.customer_id = c.id ORDER BY txn_time DESC;
