SELECT id, dept_name, budget, location FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS rn FROM departments) AS t WHERE rn <= 3 ORDER BY 1 DESC;
