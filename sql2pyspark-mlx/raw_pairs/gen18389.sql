SELECT id, opened_date, CEIL(balance) AS balance_whole FROM accounts WHERE status <> 'OPEN' ORDER BY opened_date, id LIMIT 10;
