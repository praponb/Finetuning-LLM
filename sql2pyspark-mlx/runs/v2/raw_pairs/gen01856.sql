WITH Ranked AS (SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY hire_date DESC) AS pos FROM employees) SELECT id, dept_id, hire_date FROM Ranked WHERE pos <= 3;
