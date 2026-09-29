WITH department_stats AS (
    SELECT dept_id, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    WHERE level IN ('L5', 'L3')
    GROUP BY dept_id
)
SELECT departments.id, departments.dept_name, department_stats.stddev_salary
FROM departments
LEFT OUTER JOIN department_stats ON departments.id = department_stats.dept_id
WHERE department_stats.stddev_salary > 38
ORDER BY id ASC;
