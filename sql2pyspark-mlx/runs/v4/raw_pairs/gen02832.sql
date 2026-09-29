SELECT id, hire_date, dept_id
FROM employees
WHERE hire_date >= (SELECT MAX(hire_date) FROM employees);
