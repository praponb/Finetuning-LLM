SELECT email
FROM customers
EXCEPT
SELECT email
FROM employees
WHERE MONTH(hire_date) IN (6, 10, 11);
