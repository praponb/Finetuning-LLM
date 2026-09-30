SELECT
    emp.hire_date AS employee_hire_date,
    mgr.salary AS manager_salary,
    mgr.job_title AS manager_job_title,
    mgr.full_name AS manager_full_name
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE LOWER(TRIM(mgr.full_name)) = 'olivia garcia';
