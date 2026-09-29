SELECT * FROM (SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) pos FROM departments) r WHERE pos <= 1 ORDER BY id DESC;
