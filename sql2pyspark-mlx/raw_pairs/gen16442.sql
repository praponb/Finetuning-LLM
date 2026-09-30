SELECT a.id, a.opened_date, a.customer_id FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount IS NOT NULL) ORDER BY a.customer_id DESC;
