SELECT grade_status, term, enrolled_at
FROM enrollments
WHERE score > 13.9
  AND enrolled_at >= TIMESTAMP '2025-01-31 09:00:00'
UNION ALL
SELECT term AS grade_status, grade_status AS term, enrolled_at
FROM enrollments
