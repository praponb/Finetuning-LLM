SELECT a.id, a.customer_id
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE MONTH(signup_date) IN (4, 6, 12));
