SELECT QUARTER(hire_date) AS hire_quarter, MIN(hire_date) AS first_hire_date
FROM employees
WHERE dept_id IN (8, 15, 30) AND full_name IS NULL AND manager_id IS NOT NULL
GROUP BY hire_quarter;
