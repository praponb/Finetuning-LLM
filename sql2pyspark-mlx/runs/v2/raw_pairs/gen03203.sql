WITH department_stats AS (
    SELECT dept_id, MAX(salary) - MIN(salary) salary_range, COUNT(CASE WHEN level = 'L5' THEN 1 END) l5_count
    FROM employees
    WHERE LENGTH(full_name) >= 13
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    department_stats.salary_range,
    department_stats.l5_count
FROM departments
LEFT JOIN department_stats ON departments.id = department_stats.dept_id
