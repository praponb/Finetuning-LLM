SELECT
    e.email AS employee_email,
    e.job_title AS employee_job_title,
    e.salary AS employee_salary,
    m.full_name AS manager_full_name,
    m.dept_id AS manager_dept_id
FROM employees e
INNER JOIN employees m ON e.manager_id = m.id;
