WITH scoped_courses AS (
    -- Scope the courses rows
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        LENGTH(TRIM(course_code)) AS course_code_len
    FROM courses
    WHERE credits >= 2
      AND level IN ('intro', 'core')
),
screened AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM scoped_courses s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND TO_DATE(e.enrolled_at) < DATE '2025-02-18'
        )
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY credits ASC NULLS LAST), 3) AS credits_cume_dist,
        SIZE(COLLECT_SET(dept) OVER (PARTITION BY level, credits)) AS distinct_dept_in_level_credits,
        credits - LAG(credits) OVER (ORDER BY course_code_len, id) AS credits_change
    FROM screened
)
SELECT
    dept,
    level,
    distinct_dept_in_level_credits,
    GROUPING(dept) AS is_total_dept,
    ARRAY_SORT(COLLECT_SET(course_code)) AS course_code_list
FROM windowed
GROUP BY CUBE(dept, level, distinct_dept_in_level_credits)
ORDER BY dept NULLS LAST, level NULLS LAST, distinct_dept_in_level_credits NULLS LAST
