SELECT
    e.manager_id,
    e.email,
    e.salary,
    d.budget
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id
WHERE e.job_title != 'Analyst' AND MONTH(e.hire_date) IN (1, 8, 11);
