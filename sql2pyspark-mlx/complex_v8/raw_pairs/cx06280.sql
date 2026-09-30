SELECT course_id AS key_id, enrolled_at, 'enrollment' AS record_type
FROM enrollments
UNION
SELECT student_id, enrolled_at, 'enrollment_2' AS record_type
FROM enrollments
WHERE term = '2025T1'
  AND course_id IS NOT NULL
ORDER BY enrolled_at DESC
