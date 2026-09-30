SELECT e.job_title AS employee_job_title, m.salary AS manager_salary, m.dept_id AS manager_dept_id, m.level AS manager_level FROM employees e LEFT JOIN employees m ON e.manager_id = m.id;
