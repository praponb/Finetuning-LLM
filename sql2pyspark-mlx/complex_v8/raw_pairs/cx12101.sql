SELECT
    course_id AS key_id,
    score AS value,
    enrolled_at,
    'enrollment' AS source
FROM enrollments
WHERE course_id IS NOT NULL
  AND student_id IS NOT NULL
UNION
SELECT
    course_id AS key_id,
    score AS value,
    enrolled_at,
    'enrollment_2' AS source
FROM enrollments
WHERE term IN ('2025T1', '2024T1', '2024T2')
