SELECT id, level, COUNT(*) OVER (PARTITION BY level) AS level_count FROM employees
