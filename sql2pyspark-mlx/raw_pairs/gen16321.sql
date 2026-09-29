SELECT a.account_type, c.city, c.name FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE a.account_type IN ('CREDIT', 'LOAN', 'SAVINGS');
