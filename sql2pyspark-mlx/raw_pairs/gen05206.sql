SELECT email
FROM customers
UNION
SELECT email
FROM employees
WHERE level IN ('L2', 'L3', 'L1')
ORDER BY email;
