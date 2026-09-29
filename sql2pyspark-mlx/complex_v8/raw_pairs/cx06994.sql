WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE score < 90.9
),
with_prev AS (
    -- Previous enrolled_at per course_id
    SELECT *, LAG(enrolled_at) OVER (PARTITION BY course_id ORDER BY enrolled_at, id) AS prev_enrolled_at
    FROM filtered_enrollments
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_enrolled_at IS NULL OR enrolled_at > prev_enrolled_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY course_id ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
exploded_detail AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.enrolled_at,
        s.is_new_session,
        s.session_no,
        e.id AS enrollment_id,
        e.enrolled_at AS enrollment_enrolled_at,
        e.grade_status AS enrollment_grade_status,
        e.student_id AS enrollment_student_id,
        e.score AS enrollment_score
    FROM sessionized s
    JOIN enrollments e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    enrolled_at,
    is_new_session,
    session_no,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_grade_status,
    enrollment_student_id
FROM exploded_detail
WHERE enrollment_id IS NOT NULL
ORDER BY enrollment_student_id DESC, enrollment_enrolled_at, id, enrollment_id
LIMIT 20
