SELECT d.id, d.location, d.dept_name FROM departments d WHERE d.id IN (SELECT dept_id FROM employees WHERE hire_date BETWEEN '2024-01-01' AND '2024-08-02') ORDER BY dept_name, d.location
