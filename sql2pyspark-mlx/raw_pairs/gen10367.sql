SELECT id, opened_date, account_type, balance FROM accounts WHERE opened_date <= (SELECT MIN(opened_date) FROM accounts WHERE status IN ('CLOSED', 'FROZEN', 'OPEN'))
