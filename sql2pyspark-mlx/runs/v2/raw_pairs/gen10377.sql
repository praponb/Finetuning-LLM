SELECT
    name,
    age,
    country,
    DATE_SUB(signup_date, 10) AS signup_date_minus_10d
FROM customers
WHERE credit_limit <= 7700
