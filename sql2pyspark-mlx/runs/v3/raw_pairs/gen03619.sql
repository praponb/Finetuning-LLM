SELECT * FROM accounts WHERE status IN ('FROZEN', 'CLOSED') ORDER BY id DESC, opened_date;
