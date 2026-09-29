WITH base AS (
    -- Scope the courses rows
    SELECT *
    FROM courses
    WHERE level = 'intro'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(credits) OVER (PARTITION BY dept, credits), 2) AS dept_credits_avg_credits,
        NTILE(4) OVER (PARTITION BY credits, dept ORDER BY credits DESC, id) AS credits_quartile,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY level)) AS distinct_dept_in_level,
        RANK() OVER (PARTITION BY credits, dept ORDER BY credits DESC, id) AS credits_rank,
        LEAD(credits, 2) OVER (PARTITION BY level ORDER BY credits, id) AS next2_credits,
        SIZE(COLLECT_SET(level) OVER w_dept_credits) AS distinct_level_in_dept_credits,
        SUM(credits) OVER (PARTITION BY level) AS level_sum_credits,
        NTH_VALUE(credits, 2) OVER (PARTITION BY level ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        LEAD(credits) OVER (PARTITION BY level ORDER BY credits, id) AS next_credits,
        MAX(credits) OVER (ORDER BY credits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS credits_max_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY dept, level ORDER BY credits, id), 3) AS credits_cume_dist,
        NTILE(3) OVER (PARTITION BY level ORDER BY credits, id) AS credits_tercile,
        FIRST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY level ORDER BY credits DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        MIN(credits) OVER w_level AS level_min_credits
    FROM base
    WINDOW w_dept_credits AS (PARTITION BY dept, credits), w_level AS (PARTITION BY level)
)
SELECT
    id,
    course_code,
    dept,
    credits,
    level,
    next2_credits,
    distinct_level_in_dept_credits,
    second_credits,
    next_credits,
    credits_cume_dist,
    credits_tercile,
    INITCAP(TRIM(course_code)) AS course_code_clean,
    COALESCE(dept, 'UNKNOWN') AS dept_or_unknown
FROM ranked
ORDER BY credits, level NULLS LAST
