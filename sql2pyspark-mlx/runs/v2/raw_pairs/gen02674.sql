SELECT opened_date, account_type FROM accounts WHERE account_type IN ('CREDIT', 'LOAN', 'SAVINGS') UNION ALL SELECT opened_date, account_type FROM accounts WHERE customer_id = 33;
