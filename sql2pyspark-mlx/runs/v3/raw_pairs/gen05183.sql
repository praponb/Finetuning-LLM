SELECT email, dept_id, hire_date
FROM employees
WHERE salary > 135000 AND manager_id IS NOT NULL;
