WITH department_stats AS (
    SELECT dept_id, ROUND(AVG(salary), 2) avg_salary, ROUND((100 * SUM(CASE WHEN job_title = 'Intern' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_intern
    FROM employees
    WHERE job_title IN ('Director', 'Designer', 'Analyst')
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.location,
    departments.dept_name,
    department_stats.avg_salary,
    department_stats.pct_intern
FROM departments
INNER JOIN department_stats ON departments.id = department_stats.dept_id
WHERE department_stats.pct_intern > 55
ORDER BY location DESC, id
LIMIT 5;
