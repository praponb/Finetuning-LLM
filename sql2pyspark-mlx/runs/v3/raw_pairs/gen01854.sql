SELECT id, country, email
FROM customers
WHERE credit_limit >= 14900
EXCEPT
SELECT id, country, email
FROM customers
WHERE email LIKE '%@GMAIL.COM'
ORDER BY country;
