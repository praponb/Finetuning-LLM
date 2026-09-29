SELECT a.id, a.status, a.customer_id FROM accounts a WHERE a.customer_id IS NULL AND a.customer_id IN (SELECT id FROM customers WHERE YEAR(signup_date) = 2025) ORDER BY id DESC, a.status DESC
