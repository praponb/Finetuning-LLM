WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY manager_id ORDER BY salary) AS rnk FROM employees) SELECT id, manager_id, salary FROM ranked WHERE rnk <= 2;
