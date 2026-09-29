SELECT e.job_title AS employee_job_title, e.is_remote AS employee_is_remote, m.is_remote AS manager_is_remote FROM employees AS e LEFT OUTER JOIN employees AS m ON e.manager_id = m.id;
