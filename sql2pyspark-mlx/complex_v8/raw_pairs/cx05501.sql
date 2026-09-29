SELECT credits, level
FROM courses
INTERSECT
SELECT course_id, term
FROM enrollments
WHERE enrolled_at < TIMESTAMP '2025-02-12 10:00:00'
  AND score <= 83.1
