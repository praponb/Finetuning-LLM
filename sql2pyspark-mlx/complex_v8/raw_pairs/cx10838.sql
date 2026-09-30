WITH prepared AS (
    -- Scope the courses rows
    SELECT
        id,
        dept,
        credits,
        level
    FROM courses
    WHERE level = 'core'
),
grouped AS (
    -- Group-level totals
    SELECT
        level,
        dept,
        credits,
        ROUND(100 * COUNT(CASE WHEN level IN ('intro', 'core') THEN 1 END) / COUNT(*), 1) AS pct_level_in_intro_core,
        COUNT(DISTINCT level) AS distinct_levels,
        COUNT(*) AS n_rows,
        SUM(credits) AS total_credits,
        ARRAY_SORT(COLLECT_SET(dept)) AS dept_list
    FROM prepared
    GROUP BY level, dept, credits
    HAVING MAX(credits) > 2
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE distinct_levels > (SELECT MIN(distinct_levels) FROM grouped)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        level,
        dept,
        credits,
        distinct_levels,
        n_rows,
        total_credits,
        dept_list,
        COUNT(*) OVER (PARTITION BY dept, distinct_levels) AS rows_in_dept_distinct_levels,
        LAST_VALUE(total_credits) IGNORE NULLS OVER (ORDER BY pct_level_in_intro_core, level, dept, credits ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_total_credits,
        NTILE(4) OVER (ORDER BY pct_level_in_intro_core, level, dept, credits) AS pct_level_in_intro_core_quartile,
        ROW_NUMBER() OVER (ORDER BY pct_level_in_intro_core, level, dept, credits) AS pct_level_in_intro_core_seq,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY dept, distinct_levels)) AS distinct_level_in_dept_distinct_levels
    FROM with_activity
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        level,
        dept,
        credits,
        distinct_levels,
        n_rows,
        rows_in_dept_distinct_levels,
        last_known_total_credits,
        pct_level_in_intro_core_quartile,
        pct_level_in_intro_core_seq,
        distinct_level_in_dept_distinct_levels,
        SUM(pct_level_in_intro_core_seq) OVER w_level_dept AS level_dept_sum_pct_level_in_intro_core_seq,
        SUM(last_known_total_credits) OVER (PARTITION BY level, dept) AS level_dept_sum_last_known_total_credits
    FROM with_trends
    WINDOW w_level_dept AS (PARTITION BY level, dept)
)
-- Final projection
SELECT
    level,
    dept,
    credits,
    distinct_levels,
    n_rows,
    rows_in_dept_distinct_levels,
    last_known_total_credits,
    pct_level_in_intro_core_quartile,
    distinct_level_in_dept_distinct_levels,
    level_dept_sum_last_known_total_credits,
    n_rows + last_known_total_credits AS n_rows_plus_last_known_total_credits,
    COALESCE(last_known_total_credits, 0) AS last_known_total_credits_filled
FROM with_windows
WHERE distinct_levels < 6
ORDER BY distinct_levels DESC NULLS FIRST, last_known_total_credits
