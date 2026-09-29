SELECT id, location, budget, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS row_num FROM departments WHERE dept_name IS NOT NULL;
