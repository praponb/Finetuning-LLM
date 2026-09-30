SELECT
    e.id,
    e.salary AS employee_salary,
    e.is_remote AS employee_is_remote,
    e.dept_id AS employee_dept_id,
    m.dept_id AS manager_dept_id,
    m.is_remote AS manager_is_remote,
    m.manager_id AS manager_manager_id
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE e.salary BETWEEN 73000 AND 111000 OR e.job_title = 'Analyst';
