SELECT a.id, a.account_type, a.opened_date FROM accounts a WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id) ORDER BY a.account_type ASC;
