WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        HOUR(enrolled_at) AS enrolled_at_hour,
        CONCAT(term, '#', CAST(student_id AS STRING)) AS term_ref
    FROM enrollments
    WHERE course_id >= 8
      AND grade_status IN ('INCOMPLETE', 'PASS', 'FAIL')
),
profiled AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_count
    FROM cleaned s
),
with_activity AS (
    -- Rows that have matching enrollments
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.enrolled_at >= TIMESTAMP '2025-01-25 11:00:00'
        )
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE enrollment_count < (SELECT MAX(enrollment_count) FROM with_activity)
),
with_children_enrollments AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.grade_status,
        s.enrolled_at_hour,
        s.enrollment_count,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status
    FROM screened s
    JOIN enrollments e ON e.course_id = s.course_id AND e.grade_status = 'INCOMPLETE'
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    enrollment_count,
    enrollment_enrolled_at,
    enrollment_grade_status
FROM with_children_enrollments
