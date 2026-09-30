SELECT * FROM (SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) pos FROM departments) ranked WHERE pos = 1
