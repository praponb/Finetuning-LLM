SELECT a.id, a.status, a.account_type, a.opened_date FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE NOT is_active)
