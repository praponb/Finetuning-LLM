SELECT a.id, a.account_type, a.status
FROM accounts a
WHERE a.customer_id = 27 AND a.customer_id NOT IN (SELECT id FROM customers WHERE LENGTH(name) > 6)
