WITH scoped_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        COALESCE(credits, 0) AS credits_filled,
        LOWER(TRIM(dept)) AS dept_clean,
        LENGTH(TRIM(level)) AS level_len
    FROM courses
    WHERE credits >= 2
      AND dept = 'Bio'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(credits_filled) OVER (PARTITION BY level_len ORDER BY level_len, id) AS next_credits_filled,
        ROUND(credits_filled - AVG(credits_filled) OVER w_credits, 2) AS credits_filled_vs_credits_avg,
        SUM(credits_filled) OVER (PARTITION BY credits ORDER BY level_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credits_filled
    FROM scoped_courses
    WINDOW w_credits AS (PARTITION BY credits)
)
SELECT
    credits,
    level_len,
    credits_filled,
    GROUPING_ID() AS gid,
    ROUND(SUM(running_credits_filled) / NULLIF(SUM(next_credits_filled), 0), 3) AS running_credits_filled_to_next_credits_filled_ra,
    ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list
FROM with_trends
GROUP BY CUBE(credits, level_len, credits_filled)
