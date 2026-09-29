SELECT d.dept_name, COUNT(e.id) AS employee_count, MAX(e.hire_date) AS latest_hire_date FROM departments d LEFT JOIN employees e ON d.id = e.dept_id GROUP BY d.dept_name ORDER BY employee_count DESC;
