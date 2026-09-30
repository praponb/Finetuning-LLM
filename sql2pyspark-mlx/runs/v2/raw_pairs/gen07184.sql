SELECT id, opened_date, account_type, balance FROM accounts WHERE opened_date = (SELECT MIN(opened_date) FROM accounts WHERE opened_date > '2025-08-17') ORDER BY account_type, id LIMIT 20
