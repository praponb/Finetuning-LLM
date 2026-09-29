SELECT id, dept_name, location FROM departments QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) = 1;
