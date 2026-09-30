SELECT a.id, a.opened_date, a.balance, a.customer_id FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
