WITH filtered_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status
    FROM enrollments
    WHERE course_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    term,
    grade_status,
    CONCAT_WS('-', grade_status, term) AS grade_status_term_label,
    SUBSTRING(TRIM(grade_status), 1, 2) AS grade_status_prefix
FROM filtered_enrollments
WHERE term = '2024T1'
