SELECT
    e.id,
    e.level AS employee_level,
    e.email AS employee_email,
    m.salary AS manager_salary,
    COALESCE(e.job_title, 'unassigned') AS job_title_filled
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE e.level = 'L4' OR e.full_name IS NOT NULL;
