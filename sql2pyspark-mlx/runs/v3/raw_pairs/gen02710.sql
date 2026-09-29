WITH src AS (
    SELECT *, COALESCE(full_name, 'N/A') AS full_name_filled
    FROM employees
    WHERE dept_id IN (5, 11) AND level <> 'L3'
),
employees_stats AS (
    SELECT level, MONTH(hire_date) AS hire_month, MAX(salary) AS max_salary
    FROM src
    GROUP BY level, MONTH(hire_date)
)
SELECT *
FROM employees_stats
ORDER BY hire_month, level
LIMIT 20;
