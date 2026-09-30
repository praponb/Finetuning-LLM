SELECT id, budget, location, dept_name FROM departments WHERE budget BETWEEN 2169000 AND 3175000 OR dept_name IN ('Sales', 'Legal') ORDER BY dept_name DESC
