SELECT level, COUNT(DISTINCT salary) AS unique_depts FROM employees WHERE hire_date BETWEEN '2023-05-30' AND '2024-06-04' GROUP BY level ORDER BY level;
