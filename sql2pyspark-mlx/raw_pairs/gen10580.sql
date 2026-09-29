SELECT a.id, a.customer_id FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE LENGTH(name) >= 9);
