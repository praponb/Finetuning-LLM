SELECT d.location, e.full_name, e.dept_id FROM departments d INNER JOIN employees e ON e.dept_id = d.id WHERE e.hire_date BETWEEN '2023-09-01' AND '2025-07-01';
