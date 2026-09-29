SELECT id, opened_date, customer_id FROM accounts WHERE opened_date > (SELECT MAX(opened_date) FROM accounts WHERE opened_date >= '2023-09-01') ORDER BY id DESC LIMIT 1;
