SELECT id, location, budget, dept_name FROM (SELECT id, location, budget, dept_name, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn FROM departments) AS t WHERE rn = 1
