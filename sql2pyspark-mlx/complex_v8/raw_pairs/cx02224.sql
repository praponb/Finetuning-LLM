SELECT credits AS qty
FROM courses
WHERE level = 'core'
INTERSECT ALL
SELECT course_id AS qty
FROM enrollments
ORDER BY qty
