SELECT course_id, enrolled_at, score AS metric
FROM enrollments
WHERE enrolled_at < TIMESTAMP '2025-02-12 12:00:00'
UNION
SELECT course_id, enrolled_at, score
FROM enrollments
ORDER BY metric
