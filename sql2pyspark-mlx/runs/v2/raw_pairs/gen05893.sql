SELECT id, location, budget, dept_name FROM (SELECT id, location, budget, dept_name, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) row_num FROM departments) t WHERE row_num = 1
