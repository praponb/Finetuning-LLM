SELECT account_type, opened_date FROM accounts WHERE balance >= 60000 AND status <> 'OPEN' AND status IN ('FROZEN', 'OPEN');
