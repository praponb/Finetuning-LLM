SELECT dept_id, email
FROM employees
WHERE MONTH(hire_date) IN (4, 5, 7)
UNION ALL
SELECT dept_id, email
FROM employees
WHERE salary > 123000;
