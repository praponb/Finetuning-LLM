SELECT status FROM accounts WHERE account_type = 'LOAN' UNION SELECT status FROM subscriptions WHERE MONTH(end_date) IN (4, 5, 9) ORDER BY status
