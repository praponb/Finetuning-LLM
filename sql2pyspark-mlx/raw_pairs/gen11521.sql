SELECT id, opened_date, status FROM accounts WHERE opened_date >= (SELECT MAX(opened_date) FROM accounts WHERE account_type NOT IN ('CREDIT', 'LOAN'))
