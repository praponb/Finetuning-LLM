SELECT id, opened_date, customer_id, balance FROM accounts WHERE opened_date = (SELECT MAX(opened_date) FROM accounts WHERE opened_date > '2024-04-01') ORDER BY balance DESC, id LIMIT 10;
