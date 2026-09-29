SELECT
    id,
    signup_date,
    age,
    credit_limit
FROM customers
WHERE signup_date = (SELECT MAX(signup_date) FROM customers)
