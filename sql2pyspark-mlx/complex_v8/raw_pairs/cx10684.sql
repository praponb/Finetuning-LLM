WITH recent AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE course_id >= 5
)
SELECT
    term,
    enrolled_at,
    grade_status,
    'enrollment' AS origin
FROM enrollments
UNION
SELECT
    term,
    enrolled_at,
    grade_status,
    'enrollment_2' AS origin
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-02-12 09:00:00'
