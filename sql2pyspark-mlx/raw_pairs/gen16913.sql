SELECT d.dept_name, e.salary
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
ORDER BY e.salary;
