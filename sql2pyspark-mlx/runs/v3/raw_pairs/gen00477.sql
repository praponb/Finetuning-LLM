SELECT id, account_type, customer_id, status, SUBSTRING(account_type, 1, 2) AS account_type_prefix FROM accounts
