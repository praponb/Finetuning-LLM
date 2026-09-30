SELECT
    e.job_title employee_job_title,
    e.full_name employee_full_name,
    e.dept_id employee_dept_id,
    m.salary manager_salary
FROM employees AS e
JOIN employees AS m ON e.manager_id = m.id
WHERE e.is_remote = TRUE AND m.level IN ('L2', 'L1', 'L5')
