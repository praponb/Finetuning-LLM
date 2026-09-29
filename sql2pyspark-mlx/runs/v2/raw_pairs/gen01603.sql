SELECT a.id, a.customer_id, a.account_type, a.opened_date FROM accounts a WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY account_type, id LIMIT 10;
