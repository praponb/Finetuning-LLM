SELECT a.id, a.account_type, c.credit_limit FROM accounts a INNER JOIN customers c ON a.customer_id = c.id WHERE c.is_active;
