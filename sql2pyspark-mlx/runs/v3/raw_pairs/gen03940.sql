SELECT
    emp.id,
    emp.hire_date employee_hire_date,
    mgr.dept_id manager_dept_id,
    mgr.salary manager_salary,
    mgr.level manager_level,
    UPPER(mgr.level) level_upper
FROM employees AS emp
LEFT OUTER JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE emp.full_name NOT LIKE 'K%'
ORDER BY level_upper;
