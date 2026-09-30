SELECT id, location, budget, dept_name FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rnk FROM departments WHERE budget <= 1344000) AS t WHERE rnk <= 5
