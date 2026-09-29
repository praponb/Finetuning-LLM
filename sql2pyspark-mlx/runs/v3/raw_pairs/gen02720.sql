SELECT a.id, a.opened_date, a.balance FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY a.id DESC;
