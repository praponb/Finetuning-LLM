SELECT d.budget, d.location, e.salary, e.full_name, e.manager_id FROM departments d LEFT JOIN employees e ON e.dept_id = d.id;
