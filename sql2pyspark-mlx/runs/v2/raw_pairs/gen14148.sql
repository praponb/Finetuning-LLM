SELECT e.id, e.email employee_email, m.job_title manager_job_title FROM employees e LEFT OUTER JOIN employees m ON e.manager_id = m.id WHERE m.dept_id IN (22, 24, 33) ORDER BY manager_job_title;
