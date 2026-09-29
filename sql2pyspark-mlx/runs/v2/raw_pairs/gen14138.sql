WITH ranked_employees AS (SELECT *, RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS pos FROM employees) SELECT id, level, hire_date FROM ranked_employees WHERE pos = 1 ORDER BY hire_date;
