SELECT e.id, e.full_name AS employee_full_name, m.job_title AS manager_job_title
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id;
