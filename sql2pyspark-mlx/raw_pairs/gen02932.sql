SELECT
    e.id,
    e.job_title AS employee_job_title,
    e.dept_id AS employee_dept_id,
    m.dept_id AS manager_dept_id,
    m.email AS manager_email
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.level NOT IN ('L3', 'L2', 'L1');
