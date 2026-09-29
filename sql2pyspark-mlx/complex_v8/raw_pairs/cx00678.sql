SELECT id AS key_id, credits AS qty
FROM courses
WHERE dept IS NOT NULL
UNION ALL
SELECT id, course_id
FROM enrollments
ORDER BY key_id
