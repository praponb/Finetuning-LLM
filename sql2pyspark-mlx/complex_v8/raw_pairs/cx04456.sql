WITH filtered AS (
    -- Filter courses
    SELECT
        id,
        dept,
        credits,
        level,
        CONCAT_WS('-', dept, level) AS dept_level_label,
        CONCAT_WS('|', course_code, dept) AS course_code_dept_label
    FROM courses
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER w_all_by_credits AS credits_rank,
        FIRST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY dept, level ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        ROUND(AVG(credits) OVER (ORDER BY credits DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS credits_avg_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY level ORDER BY credits DESC NULLS FIRST), 3) AS credits_percent_rank,
        SIZE(COLLECT_SET(level) OVER (PARTITION BY dept)) AS distinct_level_in_dept,
        MAX(credits) OVER (PARTITION BY dept) AS dept_max_credits,
        MAX(credits) OVER (PARTITION BY level ORDER BY credits DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS credits_max_6_prior,
        ROUND(AVG(credits) OVER (ORDER BY credits DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS credits_avg_3,
        COUNT(*) OVER (PARTITION BY dept) AS rows_in_dept,
        ROUND(credits - AVG(credits) OVER (PARTITION BY dept), 2) AS credits_vs_dept_avg,
        ROUND(CUME_DIST() OVER (ORDER BY credits DESC, id), 3) AS credits_cume_dist,
        NTILE(10) OVER w_all_by_credits AS credits_decile
    FROM filtered
    WINDOW w_all_by_credits AS (ORDER BY credits DESC, id), w_dept AS (PARTITION BY dept)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        credits,
        level,
        credits_avg_2,
        distinct_level_in_dept,
        credits_max_6_prior,
        credits_avg_3,
        credits_vs_dept_avg,
        CONCAT(course_code_dept_label, '#', CAST(first_known_credits AS STRING)) AS course_code_dept_label_ref
    FROM sequenced
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(distinct_level_in_dept - AVG(distinct_level_in_dept) OVER (PARTITION BY credits_max_6_prior), 2) AS distinct_level_in_dept_vs_credits_max_6_prior_av,
        MAX(credits_vs_dept_avg) OVER (PARTITION BY credits_max_6_prior) AS credits_max_6_prior_max_credits_vs_dept_avg,
        credits_vs_dept_avg - LAG(credits_vs_dept_avg) OVER (PARTITION BY credits_max_6_prior ORDER BY credits_avg_3 ASC, id) AS credits_vs_dept_avg_change,
        NTILE(10) OVER w_all_by_credits AS credits_decile,
        ROUND(AVG(credits_vs_dept_avg) OVER (PARTITION BY credits_max_6_prior ORDER BY credits_avg_3 ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS credits_vs_dept_avg_avg_4,
        ROW_NUMBER() OVER (PARTITION BY credits_max_6_prior ORDER BY credits_avg_3 NULLS LAST, id) AS seq_no,
        NTILE(10) OVER (PARTITION BY credits_max_6_prior ORDER BY credits_avg_3 NULLS LAST, id) AS credits_avg_3_decile
    FROM scored
    WINDOW w_all_by_credits AS (ORDER BY credits DESC, id)
)
-- Final projection
SELECT
    id,
    credits,
    level,
    distinct_level_in_dept,
    credits_max_6_prior,
    credits_avg_3,
    credits_vs_dept_avg,
    course_code_dept_label_ref,
    credits_max_6_prior_max_credits_vs_dept_avg,
    credits_vs_dept_avg_change,
    credits_decile,
    credits_vs_dept_avg_avg_4,
    seq_no,
    credits_avg_3_decile,
    SUBSTRING(TRIM(level), 1, 3) AS level_prefix
FROM sequenced_2
WHERE credits <= 3
ORDER BY credits_vs_dept_avg_avg_4
