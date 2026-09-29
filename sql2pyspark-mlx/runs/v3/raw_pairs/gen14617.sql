SELECT id, dept_name, budget FROM (SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk FROM departments) AS r WHERE rnk <= 2;
