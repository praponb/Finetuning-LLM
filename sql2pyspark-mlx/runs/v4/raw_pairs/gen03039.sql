SELECT a.id, a.opened_date, a.status, a.balance FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE name IS NOT NULL);
