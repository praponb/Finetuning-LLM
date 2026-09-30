WITH base_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
),
stats AS (
    -- Group-level totals
    SELECT
        level,
        dept,
        credits,
        SUM(credits) AS total_credits,
        ROUND(STDDEV(credits), 2) AS stddev_credits,
        ARRAY_SORT(COLLECT_SET(level)) AS level_list,
        COUNT(*) AS row_count,
        COUNT(DISTINCT dept) AS distinct_depts,
        ARRAY_SORT(COLLECT_SET(dept)) AS dept_list
    FROM base_courses
    GROUP BY level, dept, credits
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(level)) AS level_len,
        FLOOR(row_count / 2) * 2 AS row_count_bucket
    FROM stats
    WHERE distinct_depts <= 7
),
tagged AS (
    -- Label and score rows
    SELECT *, COALESCE(distinct_depts, 0) AS distinct_depts_filled
    FROM labeled
)
SELECT
    level,
    credits,
    stddev_credits,
    level_list,
    distinct_depts,
    dept_list,
    level_len,
    row_count_bucket,
    distinct_depts_filled
FROM tagged
ORDER BY credits
