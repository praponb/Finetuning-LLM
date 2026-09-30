SELECT e.dept_id AS employee_dept_id, e.level AS employee_level, m.salary AS manager_salary FROM employees e LEFT JOIN employees m ON e.manager_id = m.id WHERE m.job_title = 'Analyst';
