SELECT *
FROM customers
WHERE LENGTH(email) > 5 AND signup_date < '2025-07-04'
