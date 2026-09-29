SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn FROM departments WHERE location IN ('London', 'Bangkok', 'Singapore')
