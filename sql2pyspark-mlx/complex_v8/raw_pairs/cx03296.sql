WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id IS NOT NULL
),
line_level AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.score,
        s.grade_status,
        e.id AS enrollment_id,
        e.score AS enrollment_score,
        e.grade_status AS enrollment_grade_status,
        e.enrolled_at AS enrollment_enrolled_at,
        e.course_id AS enrollment_course_id
    FROM prepared s
    JOIN enrollments e ON e.student_id = s.student_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT grade_status, student_id, enrollment_grade_status, enrollment_score
    FROM line_level
) src
PIVOT (
    COUNT(enrollment_score)
    FOR enrollment_grade_status IN ('PASS', 'INCOMPLETE', 'WITHDRAWN')
)
