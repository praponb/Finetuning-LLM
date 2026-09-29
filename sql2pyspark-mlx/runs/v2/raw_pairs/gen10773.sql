SELECT d.id, d.budget FROM departments d WHERE d.budget BETWEEN 3042000 AND 3673000 AND NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.hire_date >= '2025-09-01')
