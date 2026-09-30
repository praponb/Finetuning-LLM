SELECT id, location, dept_name, budget, NTILE(10) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS bucket FROM departments WHERE budget > 1710000;
