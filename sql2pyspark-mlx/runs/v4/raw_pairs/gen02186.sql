SELECT t.txn_time, a.status, a.balance, c.email, c.age FROM transactions t INNER JOIN accounts a ON t.account_id = a.id INNER JOIN customers c ON c.id = a.customer_id
