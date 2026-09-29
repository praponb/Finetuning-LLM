WITH numbered AS (SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn FROM departments) SELECT * FROM numbered WHERE rn <= 5 ORDER BY dept_name DESC
