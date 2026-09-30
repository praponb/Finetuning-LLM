WITH department_stats AS (
    SELECT dept_id, MAX(salary) - MIN(salary) AS salary_range
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, d.location, ds.salary_range
FROM departments d
LEFT OUTER JOIN department_stats ds ON d.id = ds.dept_id
WHERE ds.salary_range > 70
ORDER BY budget
