SELECT grade_status, enrolled_at AS happened_at, 'enrollment' AS origin
FROM enrollments
WHERE term IN ('2024T2', '2024T1')
UNION
SELECT grade_status, enrolled_at, 'enrollment_2' AS origin
FROM enrollments
WHERE enrolled_at < TIMESTAMP '2025-02-12 12:00:00'
  AND course_id >= 8
ORDER BY happened_at DESC
