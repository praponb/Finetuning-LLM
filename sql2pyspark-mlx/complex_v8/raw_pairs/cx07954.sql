SELECT credits, dept
FROM courses
INTERSECT
SELECT id, dept
FROM courses
WHERE level IS NOT NULL
ORDER BY dept
