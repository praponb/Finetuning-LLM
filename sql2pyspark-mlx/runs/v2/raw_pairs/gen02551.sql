SELECT
    emp.id,
    emp.salary AS employee_salary,
    mgr.dept_id AS manager_dept_id,
    mgr.email AS manager_email,
    DATE_TRUNC('week', mgr.hire_date) AS hire_date_period
FROM employees emp
JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.salary >= 144000 AND emp.job_title NOT IN ('Manager', 'Analyst', 'Engineer')
ORDER BY emp.salary DESC
