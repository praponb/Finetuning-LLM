SELECT grade_status, enrolled_at AS happened_at
FROM enrollments
WHERE student_id IS NOT NULL
UNION ALL
SELECT term, enrolled_at
FROM enrollments
WHERE student_id <= 8
  AND enrolled_at >= TIMESTAMP '2025-01-19 14:00:00'
UNION ALL
SELECT grade_status, enrolled_at
FROM enrollments
WHERE grade_status IN ('FAIL', 'PASS')
  AND student_id <= 2
ORDER BY happened_at, grade_status
