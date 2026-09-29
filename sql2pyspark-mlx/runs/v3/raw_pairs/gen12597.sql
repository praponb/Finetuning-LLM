SELECT level, COUNT(DISTINCT salary) AS unique_levels
FROM employees
WHERE hire_date BETWEEN '2023-10-01' AND '2025-06-03'
GROUP BY 1
ORDER BY level ASC;
