SELECT id, email, hire_date FROM employees WHERE job_title NOT IN ('Analyst', 'Engineer', 'Intern') AND manager_id = 9;
