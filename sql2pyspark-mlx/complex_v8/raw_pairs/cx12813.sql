WITH cleaned AS (
    -- Filter courses
    SELECT
        id,
        course_code,
        dept,
        credits,
        level,
        INITCAP(TRIM(course_code)) AS course_code_clean,
        CONCAT_WS('-', dept, course_code) AS dept_course_code_label
    FROM courses
    WHERE dept = 'Bio'
      AND LOWER(course_code) LIKE '%cs%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(credits) OVER (PARTITION BY dept ORDER BY credits DESC, id) AS next_credits,
        ROUND(credits - AVG(credits) OVER (PARTITION BY dept), 2) AS credits_vs_dept_avg,
        COUNT(*) OVER (PARTITION BY dept) AS rows_in_dept,
        ROUND(credits / NULLIF(SUM(credits) OVER w_dept, 0), 4) AS credits_share_of_dept
    FROM cleaned
    WINDOW w_dept AS (PARTITION BY dept)
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.id
              AND e.student_id >= 6
              AND HOUR(e.enrolled_at) BETWEEN 7 AND 13
        )
      AND s.credits > 2
)
-- Final projection
SELECT
    id,
    course_code,
    credits,
    level,
    course_code_clean,
    dept_course_code_label,
    next_credits,
    credits_vs_dept_avg,
    credits_share_of_dept
FROM qualified
ORDER BY next_credits DESC, id
