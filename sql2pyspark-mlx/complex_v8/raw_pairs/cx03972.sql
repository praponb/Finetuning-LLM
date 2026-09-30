WITH prepared_students AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        is_international
    FROM students
    WHERE enrolled_on >= DATE '2024-03-15'
)
SELECT id, gpa
FROM students
WHERE cohort IN ('C2024', 'C2023', 'C2022')
UNION
SELECT student_id AS id, score AS gpa
FROM enrollments
WHERE course_id IS NOT NULL
  AND MONTH(enrolled_at) IN (1, 2)
ORDER BY id
