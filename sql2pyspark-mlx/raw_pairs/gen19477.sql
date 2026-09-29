SELECT a.id, a.status, a.account_type, a.balance FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions WHERE is_flagged) ORDER BY status DESC, id LIMIT 10;
