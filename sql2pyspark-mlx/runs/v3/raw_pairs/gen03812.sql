SELECT id, signup_date
FROM customers
WHERE signup_date = (SELECT MAX(signup_date) FROM customers)
ORDER BY signup_date, id
LIMIT 3
