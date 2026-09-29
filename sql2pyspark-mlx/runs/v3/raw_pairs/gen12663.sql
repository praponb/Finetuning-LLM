SELECT id, dept_name, budget, location FROM (SELECT id, dept_name, budget, location, RANK() OVER (PARTITION BY dept_name ORDER BY budget) pos FROM departments) r WHERE pos <= 3
