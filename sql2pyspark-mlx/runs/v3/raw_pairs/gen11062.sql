SELECT a.id, a.opened_date, c.country FROM accounts a JOIN customers c ON a.customer_id = c.id ORDER BY a.id LIMIT 50
