SELECT id, location, budget, dept_name FROM (SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rn FROM departments) AS r WHERE rn <= 3
