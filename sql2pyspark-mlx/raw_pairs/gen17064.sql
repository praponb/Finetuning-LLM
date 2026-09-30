SELECT id, signup_date, city, email FROM customers WHERE signup_date = (SELECT MAX(signup_date) FROM customers WHERE name LIKE '%Kanya%') ORDER BY email ASC;
