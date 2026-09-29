SELECT id, location, budget, NTILE(4) OVER (PARTITION BY location ORDER BY budget, id) AS quartile FROM departments
