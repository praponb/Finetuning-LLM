SELECT d.dept_name, d.location, e.full_name, e.salary FROM departments d LEFT OUTER JOIN employees e ON e.dept_id = d.id;
