SELECT
    'student' AS source,
    id AS ref_id,
    major,
    enrolled_on
FROM students
WHERE major IS NOT NULL
  AND is_international = FALSE
UNION ALL
SELECT
    'student_2' AS source,
    id AS ref_id,
    major,
    enrolled_on
FROM students
WHERE cohort NOT IN ('C2021', 'C2023')
