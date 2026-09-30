WITH recent_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        enrolled_at + INTERVAL 15 MINUTES AS enrolled_at_plus_15m
    FROM enrollments
    WHERE score <= 59.8
),
qualified AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM recent_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.grade_status NOT IN ('WITHDRAWN', 'FAIL')
        )
),
combined_details_course AS (
    -- Join courses
    SELECT
        s.*,
        c.level AS course_level,
        c.credits AS course_credits,
        c.dept AS course_dept
    FROM qualified s
    LEFT JOIN courses c ON c.id = s.course_id
),
detail_rows AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_plus_15m,
        s.course_level,
        s.course_dept,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.course_id AS enrollment_course_id
    FROM combined_details_course s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT course_id, course_level, score
    FROM detail_rows
) src
PIVOT (
    AVG(score)
    FOR course_level IN ('core', 'intro')
)
ORDER BY course_id
