WITH department_stats AS (
    SELECT dept_id, MAX(salary) - MIN(salary) salary_range
    FROM employees
    WHERE LOWER(TRIM(email)) = 'emma@outlook.com'
    GROUP BY dept_id
)
SELECT d.id, d.budget, ds.salary_range
FROM departments d
INNER JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.salary_range > 49;
