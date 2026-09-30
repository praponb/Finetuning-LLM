SELECT id, hire_date, full_name, job_title FROM employees WHERE hire_date >= (SELECT MAX(hire_date) FROM employees WHERE full_name LIKE 'C%');
