WITH base_enrollments AS (
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
    WHERE grade_status <> 'WITHDRAWN'
)
SELECT
    course_id AS ref_id,
    id AS entity_id,
    enrolled_at AS event_time,
    'enrollment' AS source
FROM base_enrollments
WHERE student_id <= 11
UNION ALL
SELECT
    id,
    student_id,
    enrolled_at,
    'enrollment_2' AS source
FROM enrollments
