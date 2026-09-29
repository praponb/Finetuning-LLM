WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        DATE_TRUNC('DAY', enrolled_at) AS enrolled_at_day,
        CONCAT_WS(': ', term, grade_status) AS term_grade_status_label,
        DATE_TRUNC('MONTH', enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE student_id IS NOT NULL
),
enrollment_stats AS (
    -- Per-student enrollments metrics
    SELECT student_id, MAX(enrolled_at) AS last_enrollment_enrolled_at
    FROM enrollments
    GROUP BY student_id
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        e.last_enrollment_enrolled_at
    FROM filtered_enrollments s
    LEFT JOIN enrollment_stats e ON e.student_id = s.student_id
)
SELECT
    id,
    student_id,
    course_id,
    term,
    score,
    enrolled_at,
    term_grade_status_label,
    enrolled_at_month,
    last_enrollment_enrolled_at,
    ROUND((UNIX_TIMESTAMP(enrolled_at_month) - UNIX_TIMESTAMP(enrolled_at)) / 3600, 1) AS hours_enrolled_at_to_enrolled_at_month,
    CONCAT(grade_status, '#', CAST(course_id AS STRING)) AS grade_status_ref
FROM with_stats
ORDER BY grade_status_ref DESC, last_enrollment_enrolled_at
