WITH filtered AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(level, 'UNKNOWN') AS level_or_unknown,
        INITCAP(TRIM(course_code)) AS course_code_clean
    FROM courses
    WHERE dept IN ('Bio', 'Math', 'Art')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY credits, level_or_unknown)) AS distinct_dept_in_credits_level_or_unknown,
        ROUND(credits - AVG(credits) OVER w_dept_level, 2) AS credits_vs_dept_level_avg,
        ROUND(AVG(credits) OVER (PARTITION BY credits ORDER BY credits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS credits_avg_4,
        NTILE(5) OVER (PARTITION BY dept ORDER BY credits NULLS LAST, id) AS credits_quintile,
        ROW_NUMBER() OVER w_credits_by_credits AS rn,
        COUNT(*) OVER w_credits_level_or_unknown AS rows_in_credits_level_or_unknown,
        RANK() OVER (PARTITION BY credits ORDER BY credits) AS credits_rank,
        LAG(credits, 2) OVER (PARTITION BY level_or_unknown ORDER BY credits NULLS LAST, id) AS prev2_credits,
        ROUND(AVG(credits) OVER w_credits_level_or_unknown, 2) AS credits_level_or_unknown_avg_credits,
        credits - LAG(credits) OVER w_credits_by_credits_2 AS credits_change,
        RANK() OVER (PARTITION BY dept ORDER BY credits NULLS LAST) AS credits_rank_2,
        MAX(credits) OVER w_dept_level AS dept_level_max_credits,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY credits, level_or_unknown)) AS distinct_level_in_credits_level_or_unknown
    FROM filtered
    WINDOW w_dept_level AS (PARTITION BY dept, level), w_credits_by_credits AS (PARTITION BY credits ORDER BY credits DESC, id), w_credits_level_or_unknown AS (PARTITION BY credits, level_or_unknown), w_credits_by_credits_2 AS (PARTITION BY credits ORDER BY credits, id)
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY credits_rank_2 ASC) AS credits_rank_2_dense_rank,
        MIN(credits_change) OVER w_distinct_level_in_credits_le AS distinct_level_in_credits_level_or_unknown_min_c,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level_or_unknown ORDER BY distinct_dept_in_credits_level_or_unknown, id), 3) AS distinct_dept_in_credits_level_or_unknown_percen,
        SUM(credits_avg_4) OVER (PARTITION BY level_or_unknown ORDER BY distinct_dept_in_credits_level_or_unknown, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credits_avg_4,
        ROUND(PERCENT_RANK() OVER w_level_or_unknown_by_distinct_3, 3) AS distinct_dept_in_credits_level_or_unknown_percen_2,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY distinct_level_in_credits_level_or_unknown)) AS distinct_dept_in_distinct_level_in_credits_level,
        SUM(credits_change) OVER (PARTITION BY distinct_level_in_credits_level_or_unknown) AS distinct_level_in_credits_level_or_unknown_sum_c,
        ROUND(AVG(credits) OVER (PARTITION BY distinct_level_in_credits_level_or_unknown), 2) AS distinct_level_in_credits_level_or_unknown_avg_c
    FROM metrics
    WINDOW w_distinct_level_in_credits_le AS (PARTITION BY distinct_level_in_credits_level_or_unknown), w_level_or_unknown_by_distinct_3 AS (PARTITION BY level_or_unknown ORDER BY distinct_dept_in_credits_level_or_unknown)
),
totals AS (
    -- Group-level totals
    SELECT
        credits_rank_2_dense_rank,
        distinct_dept_in_credits_level_or_unknown,
        credits_rank_2,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(dept)) AS dept_list,
        COUNT(DISTINCT dept) AS distinct_depts,
        ROUND(SUM(prev2_credits) / NULLIF(SUM(credits_change), 0), 3) AS prev2_credits_to_credits_change_ratio,
        COUNT(DISTINCT level_or_unknown) AS distinct_level_or_unknowns
    FROM metrics_2
    WHERE distinct_dept_in_distinct_level_in_credits_level >= 5
    GROUP BY credits_rank_2_dense_rank, distinct_dept_in_credits_level_or_unknown, credits_rank_2
)
SELECT
    credits_rank_2_dense_rank,
    distinct_dept_in_credits_level_or_unknown,
    credits_rank_2,
    cnt,
    dept_list,
    distinct_depts,
    prev2_credits_to_credits_change_ratio,
    distinct_level_or_unknowns
FROM totals
WHERE prev2_credits_to_credits_change_ratio > 3.0
ORDER BY credits_rank_2_dense_rank DESC, cnt DESC
