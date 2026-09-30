WITH Src AS (
    SELECT *, CAST(is_remote AS INT) AS is_remote_flag
    FROM employees
    WHERE dept_id IN (18, 24, 31)
),
employees_stats AS (
    SELECT is_remote, COUNT(is_remote_flag) AS is_remote_flag_count
    FROM Src
    GROUP BY is_remote
)
SELECT is_remote, is_remote_flag_count
FROM employees_stats
WHERE is_remote_flag_count > 4
ORDER BY is_remote_flag_count;
