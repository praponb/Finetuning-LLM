SELECT id
FROM courses
WHERE course_code IS NOT NULL
INTERSECT
SELECT credits AS id
FROM courses
ORDER BY id
