SELECT a.id, a.opened_date, a.customer_id FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE email LIKE '%@company.co')
