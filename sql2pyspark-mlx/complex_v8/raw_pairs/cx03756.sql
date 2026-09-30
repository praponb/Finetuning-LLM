SELECT id AS qty, enrolled_on
FROM students
UNION ALL
SELECT id AS qty, enrolled_on
FROM students
WHERE gpa >= 2.0
UNION ALL
SELECT id AS qty, enrolled_on
FROM students
ORDER BY enrolled_on DESC, qty
