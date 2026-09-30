WITH recent_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at,
        CONCAT(term, '#', CAST(course_id AS STRING)) AS term_ref,
        LENGTH(TRIM(grade_status)) AS grade_status_len
    FROM recent_enrollments
)
-- Final projection
SELECT
    id,
    student_id,
    course_id,
    score,
    grade_status_len
FROM tagged
ORDER BY course_id, student_id DESC NULLS FIRST
