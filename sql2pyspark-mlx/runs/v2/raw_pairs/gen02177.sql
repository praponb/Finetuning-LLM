SELECT id, dept_name, budget, RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS rnk FROM departments ORDER BY id ASC;
