SELECT id, is_active, name, signup_date, FLOOR(age) AS age_whole FROM customers WHERE is_active AND email LIKE '%@gmail.com';
