SELECT city, signup_date
FROM customers
WHERE is_active = FALSE
INTERSECT
SELECT city, signup_date
FROM customers
WHERE segment = 'Consumer';
