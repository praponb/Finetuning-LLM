SELECT t.id, t.account_id, a.balance, c.is_active FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id JOIN customers c ON c.id = a.customer_id;
