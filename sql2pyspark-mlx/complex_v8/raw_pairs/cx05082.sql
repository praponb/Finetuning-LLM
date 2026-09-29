SELECT grade_status AS category, score AS value, term
FROM enrollments
WHERE grade_status = 'WITHDRAWN'
UNION ALL
SELECT grade_status, score, term
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-02-06 09:00:00'
  AND score < 78.8
ORDER BY value
