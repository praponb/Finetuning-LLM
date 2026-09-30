SELECT a.id, a.account_type, a.opened_date FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE country = 'UK')
