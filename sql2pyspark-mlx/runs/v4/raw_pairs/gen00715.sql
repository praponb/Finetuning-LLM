SELECT customers.id, customers.email FROM customers WHERE NOT EXISTS (SELECT 1 FROM accounts WHERE accounts.customer_id = customers.id);
