SELECT t.amount, t.txn_time, a.account_type, c.is_active, c.email FROM transactions t RIGHT JOIN accounts a ON t.account_id = a.id INNER JOIN customers c ON a.customer_id = c.id ORDER BY c.email
