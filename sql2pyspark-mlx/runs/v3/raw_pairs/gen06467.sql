SELECT id, job_title FROM employees WHERE job_title <> 'Intern' EXCEPT SELECT id, job_title FROM employees WHERE hire_date >= '2024-04-01'
