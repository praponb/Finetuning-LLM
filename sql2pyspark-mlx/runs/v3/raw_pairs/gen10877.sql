WITH ranked_employees AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS rn FROM employees) SELECT id, level, salary FROM ranked_employees WHERE rn = 1;
