WITH prepared_enrollments AS (
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
    WHERE MONTH(enrolled_at) IN (2, 3)
)
SELECT grade_status
FROM prepared_enrollments
WHERE score >= 10.8
  AND term IS NOT NULL
INTERSECT
SELECT grade_status
FROM enrollments
WHERE score < 87.8
  AND course_id IS NOT NULL
ORDER BY grade_status
