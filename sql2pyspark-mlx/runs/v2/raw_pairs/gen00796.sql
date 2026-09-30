SELECT MIN(opened_date) min_opened_date, MIN(opened_date) first_opened_date FROM accounts WHERE customer_id IS NULL ORDER BY min_opened_date DESC, first_opened_date
