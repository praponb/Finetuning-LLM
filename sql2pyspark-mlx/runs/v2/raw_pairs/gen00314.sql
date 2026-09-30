SELECT email
FROM customers
WHERE city = 'New York'
UNION
SELECT email
FROM employees
WHERE email LIKE '%@mail.co.th'
