SELECT id, opened_date, customer_id FROM accounts WHERE opened_date = (SELECT MAX(opened_date) FROM accounts WHERE customer_id IS NULL) ORDER BY customer_id DESC
