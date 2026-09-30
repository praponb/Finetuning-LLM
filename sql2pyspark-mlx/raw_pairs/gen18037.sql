SELECT id, hire_date, manager_id, salary, ROUND(salary, 2) salary_rounded, LENGTH(level) level_length FROM employees WHERE LENGTH(email) >= 9
