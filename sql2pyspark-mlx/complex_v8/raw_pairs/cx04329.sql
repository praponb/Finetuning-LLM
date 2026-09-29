SELECT course_id
FROM enrollments
WHERE term = '2024T2'
INTERSECT
SELECT id AS course_id
FROM courses
WHERE level = 'core'
ORDER BY course_id
