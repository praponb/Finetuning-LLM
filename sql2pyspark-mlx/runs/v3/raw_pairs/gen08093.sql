SELECT *, credit_limit + age AS credit_limit_plus_age
FROM customers
WHERE YEAR(signup_date) = 2025
ORDER BY segment ASC
