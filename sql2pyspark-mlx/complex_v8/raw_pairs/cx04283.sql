WITH recent_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        enrolled_on,
        is_international
    FROM students
    WHERE (major IN ('Bio', 'Math', 'CS') OR NOT is_international)
)
SELECT id AS entity_id
FROM recent_students
WHERE enrolled_on >= DATE '2024-04-05'
  AND UPPER(TRIM(student_name)) LIKE 'J%'
MINUS
SELECT id
FROM enrollments
