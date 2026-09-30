SELECT d.id, d.location, COUNT(e.id) employee_count FROM departments d LEFT JOIN employees e ON d.id = e.dept_id WHERE e.manager_id IN (17, 37) GROUP BY d.id, d.location;
