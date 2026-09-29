SELECT id, signup_date
FROM customers
WHERE signup_date = (SELECT MAX(signup_date) FROM customers WHERE age < 45)
ORDER BY signup_date DESC;
