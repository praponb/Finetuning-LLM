SELECT credits AS qty
FROM courses
EXCEPT ALL
SELECT id
FROM students
ORDER BY qty DESC
