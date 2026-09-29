SELECT d.location, COUNT(e.id) AS employee_count, MIN(e.hire_date) AS min_hire_date FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.location;
