WITH ranked AS (SELECT id, dept_name, budget, RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS pos FROM departments) SELECT * FROM ranked WHERE pos <= 5 ORDER BY pos, 2 DESC;
