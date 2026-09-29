SELECT a.id, a.opened_date, a.balance, a.customer_id FROM accounts a WHERE a.id IN (SELECT account_id FROM transactions) ORDER BY a.customer_id DESC, a.opened_date, id LIMIT 1
