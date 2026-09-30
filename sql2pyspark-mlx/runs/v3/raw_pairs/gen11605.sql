SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS pos FROM departments WHERE dept_name = 'Marketing') AS ranked WHERE pos = 1 ORDER BY location;
