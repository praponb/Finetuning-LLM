SELECT id, signup_date, name
FROM customers
WHERE signup_date = (SELECT MIN(signup_date) FROM customers)
