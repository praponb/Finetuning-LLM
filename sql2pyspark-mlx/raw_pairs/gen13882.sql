SELECT id, dept_name FROM departments QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) = 1;
