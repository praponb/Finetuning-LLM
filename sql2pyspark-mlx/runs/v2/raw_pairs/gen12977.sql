WITH ranked_employees AS (SELECT id, manager_id, salary, level, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS pos FROM employees) SELECT * FROM ranked_employees WHERE pos <= 5;
