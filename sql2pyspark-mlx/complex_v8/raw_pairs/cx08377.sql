WITH filtered_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE level = 'intro'
      AND dept <> 'Math'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        MAX(credits) OVER (PARTITION BY level) AS level_max_credits,
        MIN(credits) OVER (PARTITION BY dept, level) AS dept_level_min_credits,
        RANK() OVER (PARTITION BY dept, level ORDER BY credits) AS credits_rank,
        COUNT(*) OVER (PARTITION BY dept, level) AS rows_in_dept_level,
        ROW_NUMBER() OVER w_level_dept_by_credits AS credits_seq,
        SUM(credits) OVER (PARTITION BY level, dept ORDER BY credits ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS credits_sum_1_prior,
        MAX(credits) OVER (ORDER BY credits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS credits_max_7
    FROM filtered_courses
    WINDOW w_level_dept_by_credits AS (PARTITION BY level, dept ORDER BY credits, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        credits,
        level_max_credits,
        dept_level_min_credits,
        credits_rank,
        rows_in_dept_level,
        credits_seq,
        credits_sum_1_prior,
        credits_max_7,
        FIRST_VALUE(dept_level_min_credits) OVER (PARTITION BY credits, dept ORDER BY credits_max_7 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_dept_level_min_credits,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY credits)) AS distinct_dept_in_credits,
        credits - LAG(credits) OVER (PARTITION BY credits_seq ORDER BY rows_in_dept_level ASC, id) AS credits_change,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY credits)) AS distinct_level_in_credits
    FROM with_windows
)
-- Final projection
SELECT
    id,
    credits,
    level_max_credits,
    dept_level_min_credits,
    credits_rank,
    rows_in_dept_level,
    credits_seq,
    credits_sum_1_prior,
    first_dept_level_min_credits,
    distinct_dept_in_credits,
    distinct_level_in_credits,
    ROUND(rows_in_dept_level / NULLIF(level_max_credits, 0), 3) AS rows_in_dept_level_per_level_max_credits
FROM sequenced
ORDER BY id
