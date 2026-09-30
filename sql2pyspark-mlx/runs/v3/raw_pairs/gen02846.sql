SELECT id, signup_date, city, name FROM customers WHERE signup_date = (SELECT MIN(signup_date) FROM customers WHERE is_active)
