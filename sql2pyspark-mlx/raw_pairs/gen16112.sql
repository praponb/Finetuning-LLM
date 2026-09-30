SELECT a.id, a.account_type, a.customer_id FROM accounts a WHERE a.customer_id = 26 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND NOT t.is_flagged) ORDER BY id DESC LIMIT 20;
