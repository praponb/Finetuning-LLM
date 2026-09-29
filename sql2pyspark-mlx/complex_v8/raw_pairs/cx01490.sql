WITH valid AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        MONTH(enrolled_at) AS enrolled_at_month
    FROM enrollments
    WHERE course_id IS NOT NULL
)
SELECT student_id
FROM valid
WHERE student_id <= 3
INTERSECT ALL
SELECT credits AS student_id
FROM courses
WHERE dept = 'Bio'
ORDER BY student_id
