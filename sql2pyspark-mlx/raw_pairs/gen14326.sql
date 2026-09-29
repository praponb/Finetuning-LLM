WITH department_stats AS (
    SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary, MAX(salary) AS max_salary
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    department_stats.stddev_salary,
    department_stats.max_salary
FROM departments
INNER JOIN department_stats ON departments.id = department_stats.dept_id
WHERE department_stats.max_salary >= 57000
ORDER BY department_stats.stddev_salary, id
LIMIT 5;
