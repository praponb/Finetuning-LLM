SELECT
    id AS entity_id,
    enrolled_on,
    student_name AS label,
    'student' AS src
FROM students
WHERE enrolled_on BETWEEN DATE '2024-01-24' AND DATE '2025-07-03'
  AND gpa <= 3.2
UNION
SELECT
    id AS entity_id,
    enrolled_on,
    cohort AS label,
    'student_2' AS src
FROM students
WHERE cohort = 'C2022'
  AND gpa >= 2.7
ORDER BY entity_id DESC
