SELECT 'enrollment' AS record_type, id, enrolled_at
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
  AND course_id >= 9
UNION ALL
SELECT 'enrollment_2' AS record_type, student_id, enrolled_at
FROM enrollments
ORDER BY id, enrolled_at
