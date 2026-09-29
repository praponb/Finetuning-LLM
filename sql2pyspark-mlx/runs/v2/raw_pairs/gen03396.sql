SELECT a.id, a.status FROM accounts a WHERE a.status = 'CLOSED' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY a.status, id DESC LIMIT 10;
