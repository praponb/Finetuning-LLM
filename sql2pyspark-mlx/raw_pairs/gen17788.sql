WITH department_stats AS (
    SELECT dept_id, MIN(hire_date) first_hire_date, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) manager_count
    FROM employees
    GROUP BY dept_id
)
SELECT
    departments.id,
    departments.budget,
    departments.location,
    department_stats.first_hire_date,
    department_stats.manager_count
FROM departments
INNER JOIN department_stats ON departments.id = department_stats.dept_id
ORDER BY manager_count ASC
