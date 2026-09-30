SELECT email
FROM customers
WHERE age BETWEEN 34 AND 38
INTERSECT
SELECT email
FROM employees
WHERE manager_id IS NULL
