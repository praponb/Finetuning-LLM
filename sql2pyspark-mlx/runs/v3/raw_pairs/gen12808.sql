WITH numbered AS (SELECT id, dept_name, budget, location, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn FROM departments) SELECT * FROM numbered WHERE rn <= 1;
