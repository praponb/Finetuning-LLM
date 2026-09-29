SELECT level
FROM courses
WHERE credits > 2
MINUS
SELECT dept AS level
FROM courses
WHERE dept = 'Art'
ORDER BY level DESC
