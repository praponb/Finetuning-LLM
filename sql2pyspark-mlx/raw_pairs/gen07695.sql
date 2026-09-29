SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rnk FROM departments WHERE dept_name = 'Marketing') AS ranked WHERE rnk <= 1 ORDER BY budget DESC, rnk
