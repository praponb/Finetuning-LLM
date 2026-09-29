SELECT emp.email AS employee_email, mgr.full_name AS manager_full_name, ROUND(emp.salary * 1.07, 2) AS salary_scaled
FROM employees emp
JOIN employees mgr ON emp.manager_id = mgr.id
ORDER BY mgr.full_name DESC, salary_scaled DESC
