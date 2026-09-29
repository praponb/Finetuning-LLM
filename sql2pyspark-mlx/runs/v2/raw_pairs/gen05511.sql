WITH ranked AS (SELECT id, dept_name, budget, location, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn FROM departments) SELECT * FROM ranked WHERE rn = 1 ORDER BY id DESC
