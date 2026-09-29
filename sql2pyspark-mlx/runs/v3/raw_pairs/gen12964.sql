SELECT a.id, a.account_type, a.opened_date, a.balance FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
