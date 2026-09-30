WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn FROM departments) SELECT * FROM numbered WHERE rn <= 2;
