SELECT id, opened_date, status FROM accounts WHERE opened_date = (SELECT MAX(opened_date) FROM accounts WHERE customer_id IS NULL);
