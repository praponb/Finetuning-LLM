SELECT d.location, e.full_name, e.salary, e.dept_id FROM departments d LEFT JOIN employees e ON d.id = e.dept_id WHERE e.salary >= 122000;
