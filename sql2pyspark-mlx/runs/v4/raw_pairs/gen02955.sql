SELECT e.salary AS employee_salary, m.job_title AS manager_job_title, SUBSTRING(m.job_title, 1, 4) AS job_title_prefix FROM employees e LEFT JOIN employees m ON e.manager_id = m.id
