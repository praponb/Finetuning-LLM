SELECT e.job_title AS employee_job_title, m.level AS manager_level, m.job_title AS manager_job_title
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
