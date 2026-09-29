WITH base_courses AS (
    -- Scope the courses rows
    SELECT *
    FROM courses
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        credits,
        FIRST_VALUE(credits) IGNORE NULLS OVER (ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        MAX(credits) OVER (PARTITION BY credits, dept ORDER BY credits, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS credits_max_7,
        RANK() OVER (ORDER BY credits DESC) AS credits_rank,
        ROUND(credits - AVG(credits) OVER (PARTITION BY credits), 2) AS credits_vs_credits_avg,
        NTILE(10) OVER w_all_by_credits AS credits_decile,
        ROW_NUMBER() OVER w_all_by_credits_2 AS seq_no,
        MIN(credits) OVER w_credits AS credits_min_credits,
        SIZE(COLLECT_SET(level) OVER w_credits) AS distinct_level_in_credits,
        ROUND(CUME_DIST() OVER w_all_by_credits_ties_2, 3) AS credits_cume_dist,
        COUNT(*) OVER (PARTITION BY credits) AS rows_in_credits,
        SUM(credits) OVER (PARTITION BY credits) AS credits_sum_credits,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY credits)) AS distinct_dept_in_credits
    FROM base_courses
    WINDOW w_all_by_credits AS (ORDER BY credits ASC, id), w_all_by_credits_2 AS (ORDER BY credits DESC, id), w_credits AS (PARTITION BY credits), w_all_by_credits_ties_2 AS (ORDER BY credits ASC)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE seq_no = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        credits,
        first_known_credits,
        credits_max_7,
        credits_vs_credits_avg,
        credits_decile,
        seq_no,
        credits_min_credits,
        distinct_level_in_credits,
        rows_in_credits,
        credits_sum_credits,
        COUNT(*) OVER (PARTITION BY credits_rank) AS rows_in_credits_rank,
        ROUND(credits_vs_credits_avg / NULLIF(SUM(credits_vs_credits_avg) OVER (PARTITION BY credits_rank), 0), 4) AS credits_vs_credits_avg_share_of_credits_rank,
        ROUND(credits_max_7 / NULLIF(SUM(credits_max_7) OVER w_credits_rank, 0), 4) AS credits_max_7_share_of_credits_rank,
        ROUND(PERCENT_RANK() OVER w_distinct_level_in_credits_cr, 3) AS seq_no_percent_rank,
        ROUND(AVG(credits_sum_credits) OVER (PARTITION BY distinct_level_in_credits ORDER BY credits_max_7 DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS credits_sum_credits_avg_3_prior,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER w_credits_rank, 0), 4) AS seq_no_share_of_credits_rank
    FROM first_rows
    WINDOW w_credits_rank AS (PARTITION BY credits_rank), w_distinct_level_in_credits_cr AS (PARTITION BY distinct_level_in_credits, credits_max_7 ORDER BY seq_no ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    credits,
    first_known_credits,
    credits_max_7,
    credits_vs_credits_avg,
    credits_decile,
    seq_no,
    credits_min_credits,
    rows_in_credits,
    credits_sum_credits,
    rows_in_credits_rank,
    credits_vs_credits_avg_share_of_credits_rank,
    seq_no_percent_rank,
    seq_no_share_of_credits_rank,
    CASE WHEN credits_max_7 >= 7 THEN 'A' WHEN credits_max_7 >= 5 THEN 'B' ELSE 'C' END AS credits_max_7_tier
FROM with_trends
ORDER BY first_known_credits, id DESC NULLS FIRST
