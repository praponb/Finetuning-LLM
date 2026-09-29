WITH filtered_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(dept, 'UNKNOWN') AS dept_or_unknown,
        LENGTH(TRIM(level)) AS level_len
    FROM courses
    WHERE dept = 'Art'
      AND level IN ('advanced', 'intro')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(level_len - AVG(level_len) OVER (PARTITION BY level, dept_or_unknown), 2) AS level_len_vs_level_dept_or_unknown_avg,
        FIRST_VALUE(level_len) OVER (PARTITION BY level_len ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_level_len,
        MAX(credits) OVER (PARTITION BY level, dept_or_unknown) AS level_dept_or_unknown_max_credits,
        NTILE(10) OVER (PARTITION BY credits, level ORDER BY level_len, id) AS level_len_decile,
        ROUND(credits / NULLIF(SUM(credits) OVER (PARTITION BY level, dept_or_unknown), 0), 4) AS credits_share_of_level_dept_or_unknown,
        level_len - LAG(level_len) OVER (PARTITION BY credits, level ORDER BY level_len, id) AS level_len_change,
        RANK() OVER w_dept_by_level_len_ties AS level_len_rank,
        LAST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY dept ORDER BY level_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits,
        ROW_NUMBER() OVER w_dept_by_level_len AS level_len_seq,
        LAG(credits, 2) OVER w_level_len_by_credits AS prev2_credits,
        ROUND(PERCENT_RANK() OVER w_dept_by_level_len_ties, 3) AS level_len_percent_rank
    FROM filtered_courses
    WINDOW w_dept_by_level_len_ties AS (PARTITION BY dept ORDER BY level_len), w_dept_by_level_len AS (PARTITION BY dept ORDER BY level_len, id), w_level_len_by_credits AS (PARTITION BY level_len ORDER BY credits ASC, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(level_len_seq - AVG(level_len_seq) OVER (PARTITION BY level_len_decile, level_len), 2) AS level_len_seq_vs_level_len_decile_level_len_avg,
        RANK() OVER (PARTITION BY level ORDER BY credits ASC) AS credits_rank,
        SUM(prev2_credits) OVER (PARTITION BY level ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev2_credits,
        MAX(level_len_decile) OVER (ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_level_len_decile_so_far,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY credits ASC) AS credits_dense_rank,
        RANK() OVER (ORDER BY credits, id) AS credits_rank_2
    FROM with_trends
),
rollup_base AS (
    -- Group-level totals
    SELECT
        level_len_decile,
        dept,
        SUM(level_len_decile) AS total_level_len_decile,
        COUNT(*) FILTER (WHERE level = 'intro') AS n_level_intro,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT level) AS distinct_levels,
        COUNT(*) FILTER (WHERE dept = 'Bio') AS n_dept_bio
    FROM with_windows
    GROUP BY level_len_decile, dept
)
SELECT
    level_len_decile,
    dept,
    n_level_intro,
    distinct_levels,
    n_dept_bio,
    CASE WHEN dept IN ('Bio', 'Math') THEN 'group_a' WHEN dept = 'Biz' THEN 'group_b' ELSE 'other' END AS dept_group
FROM rollup_base
WHERE level_len_decile < 8
ORDER BY distinct_levels
