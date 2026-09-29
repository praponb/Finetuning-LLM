SELECT id, budget, ROW_NUMBER() OVER (ORDER BY budget DESC, id) AS row_num FROM departments;
