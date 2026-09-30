SELECT accounts.id, accounts.account_type, accounts.status FROM accounts WHERE accounts.customer_id IN (SELECT id FROM customers WHERE signup_date BETWEEN '2023-05-28' AND '2024-08-01') ORDER BY id;
