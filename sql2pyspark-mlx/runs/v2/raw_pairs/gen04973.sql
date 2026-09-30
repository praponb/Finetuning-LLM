WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id) AS rn FROM employees WHERE is_remote = TRUE) SELECT id, level, hire_date FROM Numbered WHERE rn <= 3;
