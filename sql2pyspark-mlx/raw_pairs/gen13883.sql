SELECT a.id, a.account_type, a.customer_id
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE LENGTH(name) > 6)
ORDER BY id DESC;
