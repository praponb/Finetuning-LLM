SELECT accounts.id, accounts.balance FROM accounts WHERE accounts.customer_id IN (22, 39) AND accounts.id IN (SELECT account_id FROM transactions);
