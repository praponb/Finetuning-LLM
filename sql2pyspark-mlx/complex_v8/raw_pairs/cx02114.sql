WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(term, 'UNKNOWN') AS term_or_unknown,
        CONCAT_WS(' / ', grade_status, term) AS grade_status_term_label,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE course_id IS NOT NULL
),
with_counts AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT SUM(e.score) FROM enrollments e WHERE e.student_id = s.student_id) AS total_enrollment_score,
        (SELECT COUNT(*) FILTER (WHERE e.grade_status IN ('WITHDRAWN', 'FAIL')) FROM enrollments e WHERE e.student_id = s.student_id) AS enrollment_grade_status_count,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.student_id) AS last_enrollment_enrolled_at
    FROM prepared s
),
with_children AS (
    -- One row per enrollment
    SELECT
        s.id,
        s.student_id,
        s.course_id,
        s.term,
        s.score,
        s.grade_status,
        s.enrolled_at,
        s.grade_status_term_label,
        s.total_enrollment_score,
        s.enrollment_grade_status_count,
        e.id AS enrollment_id,
        e.term AS enrollment_term,
        e.enrolled_at AS enrollment_enrolled_at,
        e.student_id AS enrollment_student_id
    FROM with_counts s
    JOIN enrollments e ON e.course_id = s.course_id
)
-- Final projection
SELECT
    id,
    course_id,
    term,
    score,
    grade_status,
    enrolled_at,
    total_enrollment_score,
    enrollment_id,
    enrollment_enrolled_at,
    enrollment_student_id,
    LENGTH(TRIM(grade_status_term_label)) AS grade_status_term_label_len,
    ROUND((UNIX_TIMESTAMP(enrolled_at) - UNIX_TIMESTAMP(enrollment_enrolled_at)) / 60, 1) AS minutes_enrollment_enrolled_at_to_enrolled_at
FROM with_children
