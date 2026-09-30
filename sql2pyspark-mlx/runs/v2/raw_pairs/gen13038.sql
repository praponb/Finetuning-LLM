SELECT customers.id, customers.credit_limit FROM customers WHERE EXISTS (SELECT 1 FROM accounts WHERE accounts.customer_id = customers.id AND accounts.customer_id IN (5, 15));
