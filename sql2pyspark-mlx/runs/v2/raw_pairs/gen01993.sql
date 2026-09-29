SELECT id, level, salary, DENSE_RANK() OVER (PARTITION BY level ORDER BY salary DESC) AS dense_rnk FROM employees WHERE LENGTH(full_name) < 11
