SELECT id, signup_date, email, age FROM customers WHERE signup_date >= (SELECT MAX(signup_date) FROM customers) ORDER BY email, id LIMIT 5
