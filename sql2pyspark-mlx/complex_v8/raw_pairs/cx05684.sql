SELECT id
FROM courses
WHERE level = 'intro'
  AND credits > 2
INTERSECT ALL
SELECT id
FROM enrollments
WHERE course_id IS NOT NULL
  AND score <= 87.8
ORDER BY id
