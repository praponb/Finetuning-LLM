SELECT d.budget, d.dept_name, t.unique_levels
FROM departments d
JOIN (SELECT dept_id, COUNT(DISTINCT salary) AS unique_levels
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY d.budget
