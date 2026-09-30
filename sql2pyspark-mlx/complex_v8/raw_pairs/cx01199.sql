WITH cleaned_enrollments AS (
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
    WHERE term IS NOT NULL
)
SELECT student_id AS key_id, grade_status
FROM cleaned_enrollments
WHERE student_id IS NOT NULL
  AND score <= 73.0
INTERSECT
SELECT student_id AS key_id, grade_status
FROM enrollments
WHERE student_id IS NOT NULL
  AND course_id >= 8
