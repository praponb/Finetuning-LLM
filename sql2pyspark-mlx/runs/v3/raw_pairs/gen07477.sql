SELECT accounts.id, accounts.status FROM accounts WHERE accounts.customer_id NOT IN (SELECT id FROM customers WHERE signup_date <= '2025-08-01');
