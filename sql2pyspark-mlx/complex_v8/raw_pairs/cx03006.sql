WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_label,
        UPPER(TRIM(term)) AS term_clean
    FROM enrollments
    WHERE course_id >= 3
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrolled_at_label,
        e.id AS enrollment_id,
        e.grade_status AS enrollment_grade_status,
        e.course_id AS enrollment_course_id
    FROM scoped_enrollments s
    JOIN enrollments e ON e.student_id = s.student_id AND e.grade_status NOT IN ('FAIL', 'WITHDRAWN')
),
enriched_course AS (
    -- Join courses
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at,
        s.enrollment_id,
        s.enrollment_grade_status,
        s.enrollment_course_id,
        c.dept AS course_dept,
        c.course_code,
        c.level AS course_level
    FROM with_children_enrollments s
    LEFT JOIN courses c ON c.id = s.course_id AND LOWER(c.course_code) LIKE '%cs%'
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    grade_status,
    enrolled_at,
    enrollment_id,
    enrollment_grade_status,
    enrollment_course_id,
    course_dept,
    course_code,
    course_level
FROM enriched_course
ORDER BY course_dept, enrollment_grade_status
