SELECT c.id, c.age FROM customers c WHERE c.signup_date <= '2023-06-16' AND EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.account_type IN ('LOAN', 'SAVINGS'))
