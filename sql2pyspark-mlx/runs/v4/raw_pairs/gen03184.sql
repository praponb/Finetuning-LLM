SELECT account_type, customer_id FROM accounts EXCEPT ALL SELECT account_type, customer_id FROM accounts WHERE account_type IN ('CREDIT', 'SAVINGS', 'LOAN');
