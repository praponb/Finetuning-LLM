SELECT id, location, budget FROM (SELECT id, location, budget, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS row_num FROM departments) AS r WHERE row_num <= 2 ORDER BY budget
