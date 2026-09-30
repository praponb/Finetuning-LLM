WITH base_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CASE WHEN level IN ('intro', 'advanced') THEN 'group_a' WHEN level = 'core' THEN 'group_b' ELSE 'other' END AS level_group,
        CONCAT_WS('-', level, dept) AS level_dept_label,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM courses
    WHERE level IS NOT NULL
),
rollup_base AS (
    -- Summarise per group
    SELECT
        dept,
        credits,
        level_or_unknown,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(credits), 2) AS stddev_credits,
        ARRAY_SORT(COLLECT_SET(dept)) AS dept_list
    FROM base_courses
    WHERE credits >= 2
    GROUP BY dept, credits, level_or_unknown
    HAVING AVG(credits) > 2
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        dept,
        credits,
        level_or_unknown,
        distinct_ids,
        stddev_credits,
        SIZE(COLLECT_SET(level_or_unknown) OVER (PARTITION BY distinct_ids)) AS distinct_level_or_unknown_in_distinct_ids,
        ROUND(AVG(credits) OVER w_distinct_ids, 2) AS distinct_ids_avg_credits,
        LAG(stddev_credits, 3, 0) OVER w_dept_credits_by_distinct_ids AS prev3_stddev_credits,
        MAX(distinct_ids) OVER (PARTITION BY dept, credits ORDER BY distinct_ids, dept, credits, level_or_unknown ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_ids_max_3
    FROM rollup_base
    WINDOW w_distinct_ids AS (PARTITION BY distinct_ids), w_dept_credits_by_distinct_ids AS (PARTITION BY dept, credits ORDER BY distinct_ids, dept, credits, level_or_unknown)
)
SELECT
    dept,
    credits,
    distinct_ids,
    distinct_level_or_unknown_in_distinct_ids,
    prev3_stddev_credits,
    distinct_ids_max_3
FROM with_windows
ORDER BY prev3_stddev_credits DESC
