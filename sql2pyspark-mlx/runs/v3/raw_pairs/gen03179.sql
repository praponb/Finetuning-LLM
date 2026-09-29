SELECT
    e.id,
    e.salary employee_salary,
    e.job_title employee_job_title,
    e.manager_id employee_manager_id,
    m.job_title manager_job_title,
    m.dept_id manager_dept_id,
    CAST(e.salary AS INT) salary_int
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.id
WHERE m.dept_id IN (14, 20)
ORDER BY e.salary, e.id
LIMIT 5;
