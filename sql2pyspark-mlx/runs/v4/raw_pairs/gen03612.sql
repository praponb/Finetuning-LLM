SELECT
    id,
    city,
    email,
    age
FROM customers
WHERE credit_limit < 7300 AND (signup_date >= '2023-06-01' OR city NOT IN ('Singapore', 'Berlin', 'Bangkok'))
