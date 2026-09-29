WITH filtered_enrollments AS (
    -- Filter enrollments
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score > 38.1
      AND grade_status NOT IN ('FAIL', 'WITHDRAWN')
)
SELECT student_id, term AS name
FROM filtered_enrollments
WHERE term IS NOT NULL
UNION ALL
SELECT id AS student_id, cohort AS name
FROM students
WHERE is_international = TRUE
ORDER BY student_id DESC, name
