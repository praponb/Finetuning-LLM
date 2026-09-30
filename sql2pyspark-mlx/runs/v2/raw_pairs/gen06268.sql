SELECT a.id, a.customer_id, c.country, c.city, c.signup_date FROM accounts a INNER JOIN customers c ON a.customer_id = c.id ORDER BY id DESC;
