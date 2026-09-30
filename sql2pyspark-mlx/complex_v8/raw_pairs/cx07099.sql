SELECT student_id AS key_id, enrolled_at AS happened_at
FROM enrollments
WHERE course_id IS NOT NULL
  AND grade_status <> 'INCOMPLETE'
UNION ALL
SELECT student_id AS key_id, enrolled_at AS happened_at
FROM enrollments
WHERE course_id IS NOT NULL
  AND grade_status = 'FAIL'
ORDER BY happened_at
