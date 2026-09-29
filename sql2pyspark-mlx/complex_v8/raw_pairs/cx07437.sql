WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown
    FROM courses
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(credits) OVER (PARTITION BY level ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credits,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY level_or_unknown, level)) AS distinct_dept_in_level_or_unknown_level,
        NTILE(10) OVER w_all_by_credits AS credits_decile,
        ROUND(credits - AVG(credits) OVER (PARTITION BY dept), 2) AS credits_vs_dept_avg,
        FIRST_VALUE(credits) OVER (PARTITION BY level ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_credits,
        SIZE(COLLECT_SET(dept) OVER w_level_credits) AS distinct_dept_in_level_credits,
        RANK() OVER (PARTITION BY level ORDER BY credits) AS credits_rank,
        LEAD(credits, 3) OVER w_level_by_credits AS next3_credits
    FROM scoped
    WINDOW w_all_by_credits AS (ORDER BY credits ASC, id), w_level_credits AS (PARTITION BY level, credits), w_level_by_credits AS (PARTITION BY level ORDER BY credits DESC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        course_code,
        credits,
        level,
        level_or_unknown,
        running_credits,
        credits_decile,
        credits_vs_dept_avg,
        distinct_dept_in_level_credits,
        credits_rank,
        next3_credits,
        ROUND(credits_decile / NULLIF(first_credits, 0), 3) AS credits_decile_per_first_credits,
        UPPER(TRIM(level_or_unknown)) AS level_or_unknown_clean,
        CONCAT_WS(': ', level_or_unknown, dept) AS level_or_unknown_dept_label
    FROM metrics
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        course_code,
        credits,
        level,
        credits_vs_dept_avg,
        distinct_dept_in_level_credits,
        next3_credits,
        credits_decile_per_first_credits,
        level_or_unknown_clean,
        level_or_unknown_dept_label,
        NTILE(5) OVER w_credits_level_by_credits_dec AS credits_decile_per_first_credits_quintile,
        ROUND(CUME_DIST() OVER w_level_or_unknown_by_running, 3) AS running_credits_cume_dist,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY credits_rank, level_or_unknown)) AS distinct_level_in_credits_rank_level_or_unknown,
        MIN(credits) OVER (PARTITION BY distinct_dept_in_level_credits, credits_decile ORDER BY credits_vs_dept_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_credits_so_far,
        ROUND(AVG(next3_credits) OVER (PARTITION BY distinct_dept_in_level_credits, credits_decile ORDER BY credits_vs_dept_avg DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS next3_credits_avg_2_prior
    FROM scored
    WINDOW w_credits_level_by_credits_dec AS (PARTITION BY credits, level ORDER BY credits_decile_per_first_credits NULLS LAST, id), w_level_or_unknown_by_running AS (PARTITION BY level_or_unknown ORDER BY running_credits, id)
)
SELECT
    id,
    course_code,
    credits,
    next3_credits,
    level_or_unknown_clean,
    level_or_unknown_dept_label,
    credits_decile_per_first_credits_quintile,
    running_credits_cume_dist,
    distinct_level_in_credits_rank_level_or_unknown,
    min_credits_so_far,
    ABS(credits - 3) AS credits_distance,
    ROUND(credits_vs_dept_avg * 1.5, 2) AS credits_vs_dept_avg_adj
FROM ranked
