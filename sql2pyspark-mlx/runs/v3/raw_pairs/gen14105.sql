SELECT d.budget, e.level, e.dept_id, e.manager_id FROM departments d LEFT OUTER JOIN employees e ON d.id = e.dept_id WHERE d.location IN ('New York', 'London', 'Chiang Mai') OR d.budget > 3528000
