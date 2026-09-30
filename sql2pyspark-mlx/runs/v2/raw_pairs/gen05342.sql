SELECT t.id, t.merchant, a.opened_date FROM transactions t LEFT JOIN accounts a ON a.id = t.account_id ORDER BY a.opened_date DESC, id LIMIT 5;
