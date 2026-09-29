WITH remaining_keys AS (
    SELECT dept_id
    FROM employees
    EXCEPT
    SELECT dept_id
    FROM employees
    WHERE manager_id IS NULL
),
kept_rows AS (
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.dept_id = k.dept_id
)
SELECT
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L2' THEN 1 END), 0), 2) AS salary_per_l2,
    COUNT(*) FILTER (WHERE job_title = 'Manager') AS manager_rows,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN job_title = 'Manager' THEN 1 END), 0), 2) AS salary_per_manager
FROM kept_rows
GROUP BY activity_month
ORDER BY manager_rows DESC;
