SELECT id, customer_id, status, SUBSTRING(account_type, 1, 2) AS account_type_prefix FROM accounts WHERE customer_id = 21 AND status = 'OPEN' AND balance IS NULL;
