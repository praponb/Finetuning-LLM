WITH recent AS (
    -- Filter courses
    SELECT *
    FROM courses
    WHERE LOWER(course_code) LIKE '%05%'
      AND credits > 2
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        course_code,
        dept,
        credits,
        COUNT(*) OVER (PARTITION BY level, dept) AS rows_in_level_dept,
        NTILE(10) OVER (PARTITION BY dept ORDER BY credits ASC NULLS LAST, id) AS credits_decile,
        SIZE(COLLECT_SET(level) OVER w_credits_dept) AS distinct_level_in_credits_dept,
        LEAD(credits) OVER (PARTITION BY level ORDER BY credits DESC NULLS FIRST, id) AS next_credits,
        COUNT(*) OVER (PARTITION BY dept ORDER BY credits ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(credits) OVER (PARTITION BY credits, level ORDER BY credits NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_credits_so_far,
        RANK() OVER w_credits_level_by_credits AS credits_rank,
        ROUND(AVG(credits) OVER (ORDER BY credits, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS credits_avg_6_prior,
        RANK() OVER w_credits_level_by_credits_tie AS credits_rank_2,
        SUM(credits) OVER (PARTITION BY level, dept) AS level_dept_sum_credits,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY credits DESC NULLS FIRST, id) AS credits_seq,
        LEAD(credits) OVER w_dept_by_credits AS next_credits_2,
        LAST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY credits ORDER BY credits ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credits,
        COUNT(*) OVER w_credits_dept AS rows_in_credits_dept,
        credits - LAG(credits) OVER w_all_by_credits AS credits_change,
        ROUND(credits / NULLIF(SUM(credits) OVER w_level_dept, 0), 4) AS credits_share_of_level_dept
    FROM recent
    WINDOW w_credits_dept AS (PARTITION BY credits, dept), w_credits_level_by_credits AS (PARTITION BY credits, level ORDER BY credits NULLS LAST, id), w_credits_level_by_credits_tie AS (PARTITION BY credits, level ORDER BY credits NULLS LAST), w_dept_by_credits AS (PARTITION BY dept ORDER BY credits ASC NULLS LAST, id), w_all_by_credits AS (ORDER BY credits, id), w_level_dept AS (PARTITION BY level, dept)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        course_code,
        dept,
        credits,
        rows_in_level_dept,
        distinct_level_in_credits_dept,
        running_count,
        credits_rank,
        credits_avg_6_prior,
        credits_rank_2,
        level_dept_sum_credits,
        credits_seq,
        next_credits_2,
        credits_change,
        credits_share_of_level_dept,
        ROUND(next_credits_2 / NULLIF(SUM(next_credits_2) OVER (PARTITION BY credits_change), 0), 4) AS next_credits_2_share_of_credits_change,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY credits_change)) AS distinct_dept_in_credits_change,
        RANK() OVER w_next_credits_by_next_credits AS next_credits_rank,
        ROUND(AVG(credits_rank_2) OVER (PARTITION BY credits_change), 2) AS credits_change_avg_credits_rank_2
    FROM ranked
    WINDOW w_next_credits_by_next_credits AS (PARTITION BY next_credits ORDER BY next_credits NULLS LAST), w_credits_change AS (PARTITION BY credits_change)
),
per_group AS (
    -- Summarise per group
    SELECT
        distinct_dept_in_credits_change,
        distinct_level_in_credits_dept,
        COUNT(*) AS cnt,
        COUNT(DISTINCT dept) AS distinct_depts,
        SUM(level_dept_sum_credits) AS total_level_dept_sum_credits
    FROM windowed
    GROUP BY distinct_dept_in_credits_change, distinct_level_in_credits_dept
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        distinct_dept_in_credits_change,
        distinct_level_in_credits_dept,
        cnt,
        distinct_depts,
        total_level_dept_sum_credits,
        DENSE_RANK() OVER (PARTITION BY distinct_dept_in_credits_change, distinct_level_in_credits_dept ORDER BY total_level_dept_sum_credits DESC NULLS FIRST, distinct_dept_in_credits_change, distinct_level_in_credits_dept) AS total_level_dept_sum_credits_dense_rank,
        ROUND(AVG(distinct_depts) OVER (PARTITION BY distinct_dept_in_credits_change, distinct_level_in_credits_dept ORDER BY total_level_dept_sum_credits DESC NULLS FIRST, distinct_dept_in_credits_change, distinct_level_in_credits_dept ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS distinct_depts_avg_2,
        RANK() OVER w_distinct_dept_in_credits_cha AS total_level_dept_sum_credits_rank,
        FIRST_VALUE(cnt) OVER (PARTITION BY distinct_dept_in_credits_change, distinct_level_in_credits_dept ORDER BY total_level_dept_sum_credits DESC NULLS FIRST, distinct_dept_in_credits_change, distinct_level_in_credits_dept ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_cnt
    FROM per_group
    WINDOW w_distinct_dept_in_credits_cha AS (PARTITION BY distinct_dept_in_credits_change, distinct_level_in_credits_dept ORDER BY total_level_dept_sum_credits DESC NULLS FIRST, distinct_dept_in_credits_change, distinct_level_in_credits_dept)
)
SELECT
    distinct_dept_in_credits_change,
    distinct_level_in_credits_dept,
    cnt,
    distinct_depts,
    total_level_dept_sum_credits,
    total_level_dept_sum_credits_dense_rank,
    distinct_depts_avg_2,
    total_level_dept_sum_credits_rank,
    first_cnt,
    ROUND(cnt / NULLIF(first_cnt, 0), 3) AS cnt_per_first_cnt
FROM with_windows
ORDER BY distinct_level_in_credits_dept DESC, first_cnt DESC
