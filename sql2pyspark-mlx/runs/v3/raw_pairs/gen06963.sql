WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS pos FROM departments) SELECT * FROM ranked WHERE pos <= 1
