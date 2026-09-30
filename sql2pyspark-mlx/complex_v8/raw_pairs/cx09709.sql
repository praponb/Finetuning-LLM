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
    WHERE (grade_status IN ('PASS', 'WITHDRAWN') OR MONTH(enrolled_at) IN (1, 3))
)
SELECT student_id AS key_id, score
FROM cleaned_enrollments
UNION
SELECT id, gpa
FROM students
