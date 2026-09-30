SELECT email
FROM employees
WHERE email IS NOT NULL
UNION
SELECT city AS email
FROM stores
WHERE city IS NOT NULL
