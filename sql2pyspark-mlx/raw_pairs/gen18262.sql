WITH with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.salary,
        top1.level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY salary NULLS LAST) AS rank_in_group
        FROM with_last
    ) t
    WHERE rank_in_group = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY salary DESC NULLS FIRST, id) AS dedup_rank
    FROM ranked
    QUALIFY dedup_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(salary * 1.15, 2) AS adjusted_salary
    FROM deduped
    WHERE location IS NOT NULL
      AND dept_name = 'Finance'
)
-- Grouped metrics
SELECT
    CASE WHEN budget >= 3145000 THEN 'HIGH' ELSE 'LOW' END AS budget_tier,
    COUNT(*) AS num_rows,
    AVG(salary) FILTER (WHERE location = 'London') AS london_avg_salary,
    COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count,
    GROUPING_ID() AS gid
FROM base
GROUP BY ROLLUP (CASE WHEN budget >= 3145000 THEN 'HIGH' ELSE 'LOW' END)
HAVING london_avg_salary > 0
ORDER BY budget_tier DESC
