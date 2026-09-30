SELECT c.id, c.city, c.segment, c.email FROM customers c WHERE c.city = 'Bangkok' AND EXISTS (SELECT 1 FROM accounts a WHERE a.customer_id = c.id AND a.account_type IN ('LOAN', 'CREDIT', 'SAVINGS'));
