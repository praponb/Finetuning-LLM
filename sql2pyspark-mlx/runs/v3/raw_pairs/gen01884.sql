SELECT id, opened_date, LEAD(opened_date, 1) OVER (ORDER BY opened_date, id) next_opened_date FROM accounts WHERE status <> 'OPEN';
