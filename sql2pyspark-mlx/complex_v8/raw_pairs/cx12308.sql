SELECT id
FROM enrollments
INTERSECT ALL
SELECT credits AS id
FROM courses
WHERE dept IS NOT NULL
