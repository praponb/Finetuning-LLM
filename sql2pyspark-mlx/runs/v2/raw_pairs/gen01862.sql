SELECT id, location, budget, dept_name FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget) pos FROM departments) ranked WHERE pos <= 2
