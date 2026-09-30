WITH enriched AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.dept_id,
        e.manager_id,
        e.job_title,
        e.is_remote,
        e.level,
        d.level AS employee_level
    FROM employees e
    LEFT JOIN employees d
        ON e.manager_id = d.id
)
-- Multi-level aggregation
SELECT
    employee_level,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(CASE WHEN employee_level = 'L4' THEN 1 END) AS l4_count,
    COUNT_IF(employee_level = 'L4') AS l4_events
FROM enriched
GROUP BY employee_level
HAVING SIZE(COLLECT_SET(level)) >= 1
ORDER BY employee_level NULLS LAST;
