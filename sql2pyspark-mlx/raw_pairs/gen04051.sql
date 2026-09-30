SELECT id, opened_date, account_type FROM accounts WHERE status IN ('CLOSED', 'OPEN') AND status IN ('FROZEN', 'CLOSED', 'OPEN') ORDER BY opened_date DESC, id LIMIT 10
