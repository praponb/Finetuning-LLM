SELECT MIN(opened_date) AS first_opened_date FROM accounts WHERE opened_date > '2023-08-16' AND customer_id IN (13, 35) ORDER BY first_opened_date;
