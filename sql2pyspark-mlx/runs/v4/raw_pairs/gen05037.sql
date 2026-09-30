WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS pos FROM employees) SELECT id, level, hire_date FROM ranked WHERE pos <= 5 ORDER BY level
