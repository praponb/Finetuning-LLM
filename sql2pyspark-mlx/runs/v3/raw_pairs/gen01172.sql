SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY level ORDER BY hire_date DESC) AS rn FROM employees) AS t WHERE rn = 1 ORDER BY email DESC;
