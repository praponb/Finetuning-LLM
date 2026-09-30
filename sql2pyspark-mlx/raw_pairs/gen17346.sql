SELECT status FROM accounts WHERE account_type = 'SAVINGS' UNION SELECT status FROM subscriptions WHERE YEAR(end_date) >= 2023 ORDER BY status;
