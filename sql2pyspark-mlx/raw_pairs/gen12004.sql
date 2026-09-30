SELECT id, salary, manager_id, hire_date FROM employees WHERE salary <= (SELECT MIN(salary) FROM employees)
