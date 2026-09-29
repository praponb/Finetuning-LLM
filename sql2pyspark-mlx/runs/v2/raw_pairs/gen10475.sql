SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) pos FROM departments) r WHERE pos <= 1
