SELECT a.id, a.status, a.customer_id, a.opened_date FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.is_flagged) ORDER BY id DESC, customer_id DESC;
