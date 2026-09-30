SELECT e.manager_id, e.level, e.hire_date, d.dept_name, d.location, d.budget, ROUND(e.salary * 1.07, 2) AS salary_adjusted FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id;
