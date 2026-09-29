WITH DepartmentStats AS (
    SELECT dept_id, COUNT(*) AS total_count, SUM(salary) AS total_salary
    FROM employees
    WHERE LOWER(TRIM(email)) = 'ivy@gmail.com'
    GROUP BY dept_id
)
SELECT
    d.id,
    d.location,
    D.total_count,
    D.total_salary
FROM departments d
JOIN DepartmentStats D ON d.id = D.dept_id
WHERE D.total_count >= 7
ORDER BY D.total_count DESC, d.location
