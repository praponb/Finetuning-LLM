SELECT cohort AS label
FROM students
MINUS
SELECT level AS label
FROM courses
ORDER BY label
