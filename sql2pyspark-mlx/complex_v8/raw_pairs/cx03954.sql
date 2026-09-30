WITH filtered AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE grade_status IN ('WITHDRAWN', 'INCOMPLETE')
      AND course_id >= 3
)
SELECT 'enrollment' AS record_type, student_id AS entity_id, term AS name
FROM filtered
WHERE term = '2024T1'
UNION ALL
SELECT 'course' AS record_type, credits AS entity_id, level AS name
FROM courses
ORDER BY name DESC
