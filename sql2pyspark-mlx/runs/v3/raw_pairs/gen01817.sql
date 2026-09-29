SELECT id, job_title, FLOOR(salary) salary_whole, ROUND(salary, 0) salary_rounded FROM employees WHERE email IS NOT NULL
