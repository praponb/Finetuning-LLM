SELECT
    id,
    signup_date,
    name,
    email
FROM customers
WHERE signup_date = (SELECT MIN(signup_date) FROM customers)
