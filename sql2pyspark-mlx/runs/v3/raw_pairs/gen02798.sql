SELECT id, opened_date, customer_id, balance FROM accounts WHERE opened_date = (SELECT MIN(opened_date) FROM accounts WHERE balance < 31000) ORDER BY id ASC;
