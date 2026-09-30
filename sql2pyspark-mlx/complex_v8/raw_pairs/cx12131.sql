SELECT id, dept AS name
FROM courses
EXCEPT ALL
SELECT id, major AS name
FROM students
ORDER BY id DESC
