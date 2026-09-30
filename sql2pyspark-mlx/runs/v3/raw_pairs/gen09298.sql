SELECT a.id, a.status, a.account_type FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE name LIKE '%Bro%') ORDER BY account_type DESC, a.id LIMIT 3;
