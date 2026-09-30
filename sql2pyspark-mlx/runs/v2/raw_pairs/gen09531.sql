SELECT
    emp.id,
    emp.salary AS employee_salary,
    emp.manager_id AS employee_manager_id,
    emp.job_title AS employee_job_title,
    mgr.job_title AS manager_job_title,
    mgr.level AS manager_level,
    mgr.hire_date AS manager_hire_date
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE emp.salary BETWEEN 62000 AND 130000
