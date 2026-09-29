SELECT d.budget, d.location, t.unique_depts
FROM departments d
INNER JOIN (SELECT dept_id, COUNT(DISTINCT salary) AS unique_depts
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.unique_depts > 10;
