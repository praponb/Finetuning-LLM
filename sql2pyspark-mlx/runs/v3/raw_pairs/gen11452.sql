SELECT country, city
FROM customers
WHERE is_active
UNION
SELECT country, city
FROM customers
WHERE credit_limit BETWEEN 10800 AND 14300;
