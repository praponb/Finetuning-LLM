SELECT id, location, budget, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) row_num FROM departments;
