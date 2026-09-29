WITH filtered_employees AS (
    SELECT *
    FROM employees
    WHERE manager_id IN (1, 24, 26, 40) AND level IN ('L5', 'L1')
),
summary AS (
    SELECT
        is_remote,
        level,
        SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count,
        MAX(salary) AS max_salary
    FROM filtered_employees
    GROUP BY is_remote, level
)
SELECT is_remote, l5_count, max_salary
FROM summary
ORDER BY max_salary DESC, l5_count DESC, is_remote;
