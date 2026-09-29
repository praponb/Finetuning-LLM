SELECT
    emp.hire_date AS employee_hire_date,
    emp.salary AS employee_salary,
    emp.dept_id AS employee_dept_id,
    mgr.salary AS manager_salary
FROM employees emp
LEFT JOIN employees mgr ON emp.manager_id = mgr.id
WHERE LENGTH(emp.email) > 13;
