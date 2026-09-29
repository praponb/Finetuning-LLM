SELECT d.location, d.dept_name, d.budget, e.hire_date FROM departments d LEFT JOIN employees e ON e.dept_id = d.id ORDER BY budget DESC, d.location
