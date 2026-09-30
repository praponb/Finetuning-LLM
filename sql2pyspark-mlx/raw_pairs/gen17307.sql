SELECT a.account_type, c.country, c.city, c.signup_date FROM accounts a INNER JOIN customers c ON c.id = a.customer_id;
