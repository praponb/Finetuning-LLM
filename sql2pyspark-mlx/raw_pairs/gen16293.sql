SELECT id, opened_date, balance FROM accounts WHERE opened_date > (SELECT MAX(opened_date) FROM accounts WHERE status IN ('CLOSED', 'FROZEN', 'OPEN'));
