WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        level,
        CASE WHEN credits >= 3 THEN 'A' WHEN credits >= 2 THEN 'B' ELSE 'C' END AS credits_tier
    FROM courses
    WHERE level = 'advanced'
      AND credits < 3
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        course_code,
        dept,
        credits_tier,
        LENGTH(TRIM(course_code)) AS course_code_len,
        CONCAT_WS(': ', level, dept) AS level_dept_label,
        SUBSTRING(TRIM(level), 1, 4) AS level_prefix
    FROM base
    WHERE level = 'advanced'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        course_code,
        level_dept_label,
        NTH_VALUE(course_code_len, 2) OVER (ORDER BY course_code_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_course_code_len,
        ROUND(course_code_len / NULLIF(SUM(course_code_len) OVER (PARTITION BY course_code_len), 0), 4) AS course_code_len_share_of_course_code_len,
        SIZE(COLLECT_SET(credits_tier) OVER (PARTITION BY course_code_len)) AS distinct_credits_tier_in_course_code_len,
        ROUND(course_code_len - AVG(course_code_len) OVER (PARTITION BY course_code_len), 2) AS course_code_len_vs_course_code_len_avg
    FROM scored
)
-- Final projection
SELECT
    id,
    course_code,
    level_dept_label,
    second_course_code_len,
    course_code_len_vs_course_code_len_avg,
    SUBSTRING(TRIM(course_code), 1, 3) AS course_code_prefix
FROM metrics
WHERE level_dept_label IS NOT NULL
ORDER BY second_course_code_len DESC
