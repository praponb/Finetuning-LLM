SELECT d.budget, d.location, e.dept_id FROM departments d LEFT JOIN employees e ON e.dept_id = d.id WHERE d.budget <= 1968000 AND e.salary <= 130000 ORDER BY d.location ASC;
