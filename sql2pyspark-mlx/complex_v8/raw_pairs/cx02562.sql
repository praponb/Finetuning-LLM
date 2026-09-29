WITH prepared_students AS (
    -- Filter students
    SELECT *
    FROM students
)
SELECT cohort AS category, id, gpa AS metric
FROM prepared_students
UNION
SELECT major, id, gpa
FROM students
WHERE enrolled_on >= DATE '2024-04-01'
ORDER BY metric DESC, category
