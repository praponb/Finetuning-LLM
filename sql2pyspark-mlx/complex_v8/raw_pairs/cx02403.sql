SELECT level
FROM courses
WHERE level = 'intro'
INTERSECT ALL
SELECT dept AS level
FROM courses
