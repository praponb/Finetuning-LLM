WITH numbered AS (SELECT id, location, budget, dept_name, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS pos FROM departments) SELECT * FROM numbered WHERE pos <= 3
