SELECT d.budget, e.full_name, e.hire_date, LOWER(d.location) AS location_lower FROM departments d LEFT JOIN employees e ON e.dept_id = d.id
