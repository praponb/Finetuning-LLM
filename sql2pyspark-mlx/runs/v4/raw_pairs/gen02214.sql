SELECT a.id, a.status, a.opened_date, a.account_type FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE name LIKE '%Brow%')
