WITH valid AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        CONCAT(course_code, '#', CAST(id AS STRING)) AS course_code_ref,
        CASE WHEN dept IN ('Math', 'CS') THEN 'group_a' WHEN dept = 'Art' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(credits) OVER w_level_dept_group AS level_dept_group_sum_credits,
        ROUND(AVG(credits) OVER (ORDER BY credits ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS credits_avg_3,
        ROUND(credits - AVG(credits) OVER (PARTITION BY level, dept_group), 2) AS credits_vs_level_dept_group_avg,
        MAX(credits) OVER w_level_dept_group AS level_dept_group_max_credits,
        FIRST_VALUE(credits) IGNORE NULLS OVER (ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        NTILE(3) OVER w_all_by_credits AS credits_tercile,
        LEAD(credits) OVER (ORDER BY credits ASC, id) AS next_credits,
        ROUND(PERCENT_RANK() OVER (ORDER BY credits DESC), 3) AS credits_percent_rank,
        ROUND(AVG(credits) OVER w_level_dept_group, 2) AS level_dept_group_avg_credits,
        COUNT(*) OVER (PARTITION BY level, dept_group) AS rows_in_level_dept_group,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY level, dept_group)) AS distinct_dept_in_level_dept_group,
        LAG(credits, 3) OVER w_all_by_credits AS prev3_credits
    FROM valid
    WINDOW w_level_dept_group AS (PARTITION BY level, dept_group), w_all_by_credits AS (ORDER BY credits ASC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        level,
        next_credits,
        credits,
        COUNT(*) AS row_count,
        COUNT(DISTINCT dept) AS distinct_depts,
        MIN(next_credits) AS min_next_credits,
        ROUND(AVG(credits_percent_rank), 2) AS avg_credits_percent_rank,
        SUM(credits_tercile) AS total_credits_tercile,
        COUNT(DISTINCT dept_group) AS distinct_dept_groups
    FROM sequenced
    WHERE prev3_credits < 3
    GROUP BY level, next_credits, credits
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY min_next_credits ORDER BY min_next_credits DESC) AS min_next_credits_dense_rank,
        RANK() OVER (PARTITION BY distinct_dept_groups, distinct_depts ORDER BY total_credits_tercile DESC) AS total_credits_tercile_rank,
        NTH_VALUE(min_next_credits, 2) OVER (PARTITION BY min_next_credits ORDER BY min_next_credits DESC, level, next_credits, credits ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_next_credits
    FROM rollup_base
)
-- Final projection
SELECT
    level,
    next_credits,
    row_count,
    distinct_depts,
    min_next_credits,
    avg_credits_percent_rank,
    total_credits_tercile,
    distinct_dept_groups,
    total_credits_tercile_rank,
    second_min_next_credits
FROM windowed
ORDER BY total_credits_tercile DESC, total_credits_tercile_rank
