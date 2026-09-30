SELECT course_id, enrolled_at AS happened_at, 'enrollment' AS record_type
FROM enrollments
UNION
SELECT course_id, enrolled_at, 'enrollment_2' AS record_type
FROM enrollments
WHERE course_id <= 4
  AND term = '2025T2'
