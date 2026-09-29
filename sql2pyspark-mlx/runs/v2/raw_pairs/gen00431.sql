SELECT a.id, a.customer_id, a.balance FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE LENGTH(name) > 8) ORDER BY a.id DESC LIMIT 3;
