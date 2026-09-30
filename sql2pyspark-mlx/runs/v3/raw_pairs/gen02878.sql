SELECT a.id, a.opened_date FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount < 2700);
