SELECT id, customer_id, opened_date, SUBSTRING(status, 1, 4) AS status_prefix FROM accounts WHERE account_type = 'LOAN' OR account_type = 'CREDIT' ORDER BY id, status_prefix
