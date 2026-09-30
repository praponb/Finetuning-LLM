WITH numbered AS (SELECT id, location, budget, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS rn FROM departments) SELECT * FROM numbered WHERE rn <= 3;
