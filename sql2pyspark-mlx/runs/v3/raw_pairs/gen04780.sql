WITH department_totals AS (
    SELECT dept_id, COUNT(DISTINCT salary) AS unique_levels, MAX(salary) AS max_salary
    FROM employees
    WHERE email LIKE '%@outlook.com'
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    dt.unique_levels,
    dt.max_salary
FROM departments d
JOIN department_totals dt ON d.id = dt.dept_id
WHERE dt.max_salary >= 97000;
