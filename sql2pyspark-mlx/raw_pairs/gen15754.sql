SELECT a.id, a.opened_date, a.customer_id FROM accounts a WHERE a.status IN ('CLOSED', 'FROZEN', 'OPEN') AND a.customer_id NOT IN (SELECT id FROM customers WHERE country = 'JP')
