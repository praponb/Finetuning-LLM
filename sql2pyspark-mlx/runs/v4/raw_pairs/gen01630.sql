SELECT COUNT(DISTINCT credit_limit) AS unique_citys, MAX(signup_date) AS last_signup_date
FROM customers
WHERE NOT (age BETWEEN 43 AND 51 AND email LIKE '%@yahoo.com' AND age IS NOT NULL)
ORDER BY 1, last_signup_date;
