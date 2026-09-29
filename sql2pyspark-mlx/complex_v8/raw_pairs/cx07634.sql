SELECT id AS qty, enrolled_on
FROM students
UNION
SELECT id, enrolled_on
FROM students
