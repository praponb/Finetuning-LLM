WITH scoped_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        SUBSTRING(TRIM(course_code), 1, 2) AS course_code_prefix,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM courses
    WHERE dept = 'Art'
      AND LOWER(course_code) LIKE '%a1%'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(credits) IGNORE NULLS OVER (PARTITION BY course_code_len, credits ORDER BY course_code_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credits,
        COUNT(*) OVER (PARTITION BY credits, dept) AS rows_in_credits_dept,
        ROUND(course_code_len / NULLIF(SUM(course_code_len) OVER (PARTITION BY credits, dept), 0), 4) AS course_code_len_share_of_credits_dept,
        DENSE_RANK() OVER (ORDER BY course_code_len, id) AS course_code_len_dense_rank,
        ROUND(AVG(course_code_len) OVER (PARTITION BY level), 2) AS level_avg_course_code_len,
        ROUND(AVG(credits) OVER w_credits_dept, 2) AS credits_dept_avg_credits,
        LAST_VALUE(credits) OVER (PARTITION BY credits ORDER BY credits, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_credits,
        credits - LAG(credits) OVER (ORDER BY course_code_len DESC, id) AS credits_change,
        MAX(credits) OVER (PARTITION BY credits, dept) AS credits_dept_max_credits,
        DENSE_RANK() OVER (PARTITION BY level ORDER BY credits DESC) AS credits_dense_rank,
        ROUND(credits - AVG(credits) OVER (PARTITION BY dept), 2) AS credits_vs_dept_avg,
        ROUND(CUME_DIST() OVER (ORDER BY course_code_len DESC), 3) AS course_code_len_cume_dist,
        SUM(course_code_len) OVER (PARTITION BY credits, dept) AS credits_dept_sum_course_code_len,
        LEAD(course_code_len, 1, 0) OVER (ORDER BY course_code_len, id) AS next_course_code_len
    FROM scoped_courses
    WINDOW w_credits_dept AS (PARTITION BY credits, dept)
)
-- Final projection
SELECT
    id,
    course_code_prefix,
    first_known_credits,
    course_code_len_share_of_credits_dept,
    level_avg_course_code_len,
    credits_dept_avg_credits,
    last_credits,
    credits_change,
    credits_dept_max_credits,
    credits_dense_rank,
    course_code_len_cume_dist,
    credits_dept_sum_course_code_len
FROM ranked
WHERE credits_dept_sum_course_code_len > 30
ORDER BY credits_dept_avg_credits DESC
