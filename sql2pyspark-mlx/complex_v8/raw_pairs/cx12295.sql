SELECT term AS category
FROM enrollments
EXCEPT
SELECT level
FROM courses
WHERE level IN ('advanced', 'core')
ORDER BY category DESC
