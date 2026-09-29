SELECT id, account_type FROM accounts WHERE customer_id = 37 OR status NOT IN ('FROZEN', 'CLOSED') ORDER BY id DESC LIMIT 5;
