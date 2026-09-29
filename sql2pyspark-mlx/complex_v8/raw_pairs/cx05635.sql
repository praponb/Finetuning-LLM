SELECT course_id AS key_id, enrolled_at
FROM enrollments
WHERE term <> '2025T2'
  AND enrolled_at >= TIMESTAMP '2025-01-25 11:00:00'
UNION
SELECT id AS key_id, enrolled_at
FROM enrollments
WHERE course_id IS NOT NULL
  AND student_id IS NOT NULL
