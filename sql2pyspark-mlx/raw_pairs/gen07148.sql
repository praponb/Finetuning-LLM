SELECT id, dept_name, budget FROM departments QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST, id) = 1;
