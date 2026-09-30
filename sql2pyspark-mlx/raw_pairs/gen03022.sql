WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT level, manager_id
    FROM employees
    INTERSECT
    SELECT level, manager_id
    FROM employees
    WHERE full_name LIKE '%Tay%'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    JOIN remaining_keys k ON s.level = k.level AND s.manager_id = k.manager_id
)
-- Multi-level aggregation
SELECT
    level,
    COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count,
    COUNT(*) FILTER (WHERE job_title = 'Designer') AS designer_rows,
    COUNT(DISTINCT level) AS distinct_levels
FROM verified
GROUP BY level
ORDER BY designer_count NULLS FIRST
