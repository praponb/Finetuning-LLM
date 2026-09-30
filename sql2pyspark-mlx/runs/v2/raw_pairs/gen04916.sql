SELECT id, signup_date, country FROM customers WHERE signup_date = (SELECT MAX(signup_date) FROM customers) ORDER BY id
