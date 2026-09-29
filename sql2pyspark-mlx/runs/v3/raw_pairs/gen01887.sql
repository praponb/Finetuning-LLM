SELECT account_type FROM accounts WHERE opened_date <= '2024-12-24' OR status NOT IN ('OPEN', 'CLOSED')
