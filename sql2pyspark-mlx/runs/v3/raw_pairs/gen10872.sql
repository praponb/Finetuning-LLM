SELECT d.budget, d.location, d.dept_name, e.hire_date, e.is_remote FROM departments d LEFT JOIN employees e ON d.id = e.dept_id WHERE e.hire_date <= '2023-03-01' ORDER BY e.hire_date ASC;
