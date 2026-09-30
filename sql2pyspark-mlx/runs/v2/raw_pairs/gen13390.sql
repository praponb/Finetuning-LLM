SELECT e.id, e.salary AS employee_salary, e.job_title AS employee_job_title, m.full_name AS manager_full_name FROM employees e LEFT JOIN employees m ON e.manager_id = m.id;
