SELECT id, major AS category
FROM students
MINUS
SELECT id, level AS category
FROM courses
ORDER BY category DESC
