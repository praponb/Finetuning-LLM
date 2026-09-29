WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget) AS row_num FROM departments) SELECT id, location, budget FROM ranked WHERE row_num = 1 ORDER BY location DESC
