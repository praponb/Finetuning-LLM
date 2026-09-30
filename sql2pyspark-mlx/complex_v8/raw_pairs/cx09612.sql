WITH recent_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international
    FROM students
)
SELECT id
FROM recent_students
WHERE student_name IS NOT NULL
EXCEPT ALL
SELECT student_id AS id
FROM enrollments
WHERE enrolled_at < TIMESTAMP '2025-02-18 10:00:00'
  AND grade_status = 'WITHDRAWN'
ORDER BY id
