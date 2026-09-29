WITH base AS (
    SELECT *, CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE manager_id IS NULL AND level IN ('L3', 'L4', 'L1')
),
Agg AS (
    SELECT job_title, SUM(CASE WHEN job_title = 'Designer' THEN is_remote_flag ELSE 0 END) AS designer_is_remote_flag, MAX(salary) - MIN(salary) AS salary_range
    FROM base
    GROUP BY job_title
)
SELECT *
FROM Agg;
