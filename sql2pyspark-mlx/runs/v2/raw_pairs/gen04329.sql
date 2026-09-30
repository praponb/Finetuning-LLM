SELECT id, level, salary, is_remote FROM (SELECT *, RANK() OVER (PARTITION BY level ORDER BY salary DESC) rnk FROM employees WHERE MONTH(hire_date) IN (1, 3, 5)) r WHERE rnk = 1 ORDER BY level;
