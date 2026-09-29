WITH ranked AS (SELECT id, level, salary, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS row_num FROM employees) SELECT id, level, salary FROM ranked WHERE row_num <= 2;
