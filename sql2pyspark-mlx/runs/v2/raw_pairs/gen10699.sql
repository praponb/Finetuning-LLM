SELECT id, dept_name, budget FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rnk FROM departments) AS ranked WHERE rnk = 1 ORDER BY dept_name, id DESC;
