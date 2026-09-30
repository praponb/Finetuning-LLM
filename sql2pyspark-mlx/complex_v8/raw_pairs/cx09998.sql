WITH filtered AS (
    -- Filter courses
    SELECT *
    FROM courses
    WHERE credits > 2
      AND dept <> 'Bio'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY credits, dept) AS rows_in_credits_dept,
        ROW_NUMBER() OVER (PARTITION BY credits, dept ORDER BY credits, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY credits, dept ORDER BY credits, id) AS credits_dense_rank,
        RANK() OVER (ORDER BY credits ASC) AS credits_rank,
        NTH_VALUE(credits, 2) OVER (ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        ROUND(AVG(credits) OVER (PARTITION BY credits, level), 2) AS credits_level_avg_credits,
        ROUND(PERCENT_RANK() OVER (ORDER BY credits ASC), 3) AS credits_percent_rank,
        NTILE(4) OVER w_credits_dept_by_credits AS credits_quartile,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY credits, level)) AS distinct_dept_in_credits_level,
        RANK() OVER (PARTITION BY credits ORDER BY credits ASC NULLS LAST) AS credits_rank_2
    FROM filtered
    WINDOW w_credits_dept_by_credits AS (PARTITION BY credits, dept ORDER BY credits, id)
)
-- Final projection
SELECT
    id,
    course_code,
    dept,
    credits,
    credits_dense_rank,
    credits_rank,
    second_credits,
    credits_level_avg_credits,
    credits_quartile,
    distinct_dept_in_credits_level,
    CONCAT_WS('-', course_code, level) AS course_code_level_label
FROM sequenced
WHERE credits BETWEEN 2 AND 4
ORDER BY credits_level_avg_credits DESC
