WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level
    FROM courses
    WHERE LOWER(course_code) LIKE '%cs%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        ROUND(credits / NULLIF(SUM(credits) OVER w_dept_level, 0), 4) AS credits_share_of_dept_level,
        RANK() OVER (PARTITION BY level, dept ORDER BY credits DESC) AS credits_rank,
        NTH_VALUE(credits, 2) OVER (ORDER BY credits ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_credits,
        ROUND(PERCENT_RANK() OVER w_credits_dept_by_credits_ties, 3) AS credits_percent_rank,
        SUM(credits) OVER (PARTITION BY credits, dept ORDER BY credits DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS credits_sum_2_prior,
        SUM(credits) OVER (PARTITION BY credits, dept ORDER BY credits DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credits,
        COUNT(*) OVER (PARTITION BY dept, level) AS rows_in_dept_level,
        ROUND(PERCENT_RANK() OVER (ORDER BY credits ASC, id), 3) AS credits_percent_rank_2,
        ROUND(credits - AVG(credits) OVER w_dept_level, 2) AS credits_vs_dept_level_avg,
        LEAD(credits, 3) OVER (PARTITION BY credits, dept ORDER BY credits DESC, id) AS next3_credits,
        RANK() OVER (ORDER BY credits ASC, id) AS credits_rank_2
    FROM cleaned
    WINDOW w_dept_level AS (PARTITION BY dept, level), w_credits_dept_by_credits_ties AS (PARTITION BY credits, dept ORDER BY credits DESC)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug,
        LENGTH(TRIM(course_code)) AS course_code_len,
        LOWER(REGEXP_REPLACE(TRIM(course_code), ' ', '_')) AS course_code_slug_2
    FROM metrics
    WHERE credits <= 4
)
SELECT
    id,
    credits,
    level,
    credits_share_of_dept_level,
    credits_rank,
    second_credits,
    running_credits,
    rows_in_dept_level,
    credits_vs_dept_level_avg,
    next3_credits,
    credits_rank_2,
    course_code_slug,
    course_code_len
FROM derived
ORDER BY credits DESC NULLS FIRST, id
LIMIT 5
