SELECT accounts.id, accounts.account_type, accounts.customer_id FROM accounts WHERE accounts.customer_id NOT IN (SELECT id FROM customers WHERE LOWER(TRIM(email)) = 'frank@company.co');
