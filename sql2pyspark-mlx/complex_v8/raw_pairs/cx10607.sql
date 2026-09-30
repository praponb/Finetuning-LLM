SELECT major AS label
FROM students
INTERSECT ALL
SELECT dept AS label
FROM courses
ORDER BY label
