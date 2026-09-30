SELECT id, manager_id, job_title, ROUND(salary, 2) AS salary_rounded, LENGTH(email) AS email_length FROM employees WHERE salary < 148000 AND MONTH(hire_date) IN (3, 4, 7)
