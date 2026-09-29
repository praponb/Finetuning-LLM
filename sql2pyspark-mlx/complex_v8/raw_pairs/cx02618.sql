SELECT id AS qty
FROM students
WHERE major = 'Art'
  AND UPPER(TRIM(student_name)) LIKE 'A%'
MINUS
SELECT id
FROM students
ORDER BY qty DESC
