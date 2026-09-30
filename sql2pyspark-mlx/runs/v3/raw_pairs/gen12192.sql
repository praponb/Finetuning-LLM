SELECT accounts.id, accounts.account_type, accounts.customer_id FROM accounts WHERE accounts.id IN (SELECT account_id FROM transactions WHERE account_id IN (14, 18, 21, 24)) ORDER BY customer_id ASC;
