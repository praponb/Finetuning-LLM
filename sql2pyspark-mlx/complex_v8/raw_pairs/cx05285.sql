WITH filtered_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        credits,
        level,
        INITCAP(TRIM(dept)) AS dept_clean
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'A%'
      AND dept <> 'Bio'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY level ORDER BY credits, id) AS credits_quartile,
        LEAD(credits) OVER (PARTITION BY credits ORDER BY credits DESC, id) AS next_credits,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY credits)) AS distinct_level_in_credits,
        NTH_VALUE(credits, 2) OVER (PARTITION BY level ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        MIN(credits) OVER w_credits AS credits_min_credits,
        RANK() OVER (PARTITION BY level, credits ORDER BY credits, id) AS credits_rank,
        credits - LAG(credits) OVER w_level_by_credits AS credits_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY credits ORDER BY credits DESC), 3) AS credits_percent_rank,
        ROUND(AVG(credits) OVER (PARTITION BY level, credits ORDER BY credits NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS credits_avg_1_prior
    FROM filtered_courses
    WINDOW w_credits AS (PARTITION BY credits), w_level_by_credits AS (PARTITION BY level ORDER BY credits, id)
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_code,
        credits,
        dept_clean,
        credits_quartile,
        next_credits,
        credits_min_credits,
        credits_rank,
        credits_change,
        credits_percent_rank,
        credits_avg_1_prior,
        COUNT(*) OVER (PARTITION BY distinct_level_in_credits) AS rows_in_distinct_level_in_credits,
        MAX(second_credits) OVER (PARTITION BY credits_rank ORDER BY credits_avg_1_prior DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS second_credits_max_6_prior,
        LAST_VALUE(credits_quartile) IGNORE NULLS OVER (PARTITION BY credits_rank, credits_quartile ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits_quartile
    FROM ranked
    WINDOW w_distinct_level_in_credits AS (PARTITION BY distinct_level_in_credits)
)
-- Final projection
SELECT
    id,
    course_code,
    credits,
    dept_clean,
    credits_quartile,
    credits_min_credits,
    credits_change,
    credits_percent_rank,
    credits_avg_1_prior,
    LOWER(REGEXP_REPLACE(TRIM(dept_clean), ' ', '_')) AS dept_clean_slug
FROM ranked_2
ORDER BY credits_percent_rank NULLS LAST
