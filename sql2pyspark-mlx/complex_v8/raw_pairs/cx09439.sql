SELECT id
FROM enrollments
INTERSECT
SELECT credits
FROM courses
WHERE dept = 'Biz'
ORDER BY id
