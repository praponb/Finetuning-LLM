WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        CONCAT_WS('|', grade_status, term) AS grade_status_term_label
    FROM enrollments
    WHERE grade_status IN ('INCOMPLETE', 'WITHDRAWN', 'PASS')
),
with_parent_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.grade_status,
        s.grade_status_term_label,
        c.course_code,
        c.level AS course_level
    FROM base s
    JOIN courses c ON c.id = s.course_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    grade_status_term_label,
    course_code,
    course_level
FROM with_parent_course
ORDER BY id, grade_status DESC
