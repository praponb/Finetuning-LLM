SELECT c.id, c.credit_limit, c.name, c.signup_date FROM customers c WHERE EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.account_type IN ('CREDIT', 'LOAN', 'SAVINGS'))
