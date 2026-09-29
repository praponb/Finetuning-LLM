SELECT id
FROM students
EXCEPT ALL
SELECT id
FROM courses
WHERE dept = 'Bio'
ORDER BY id DESC
