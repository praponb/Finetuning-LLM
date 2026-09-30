SELECT
    id,
    email,
    city,
    signup_date,
    credit_limit,
    age
FROM customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY credit_limit, id) = 1
