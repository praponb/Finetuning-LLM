SELECT
    d.id,
    d.budget,
    d.location,
    t.unique_levels,
    t.first_hire_date
FROM departments d
LEFT JOIN (SELECT dept_id, COUNT(DISTINCT salary) AS unique_levels, MIN(hire_date) AS first_hire_date
    FROM employees
    WHERE manager_id IN (16, 24)
    GROUP BY dept_id) t ON d.id = t.dept_id
WHERE t.unique_levels >= 4
ORDER BY unique_levels DESC
