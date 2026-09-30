SELECT id, salary, ADD_MONTHS(hire_date, 3) AS hire_date_plus_months FROM employees WHERE job_title = 'Intern' ORDER BY salary DESC;
