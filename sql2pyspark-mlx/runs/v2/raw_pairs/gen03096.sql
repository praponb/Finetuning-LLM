SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rnk FROM departments WHERE location IN ('Bangkok', 'London')) AS t WHERE rnk <= 5
