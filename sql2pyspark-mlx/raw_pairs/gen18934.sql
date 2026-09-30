SELECT *
FROM employees
WHERE LENGTH(email) > 9 AND full_name LIKE '%Jack%'
ORDER BY hire_date DESC
