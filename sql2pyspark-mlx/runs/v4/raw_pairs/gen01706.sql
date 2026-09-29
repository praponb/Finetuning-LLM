SELECT a.id, a.account_type, a.balance, c.is_active FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.is_active ORDER BY id
