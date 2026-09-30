SELECT id AS qty, enrolled_on
FROM students
UNION ALL
SELECT id, enrolled_on
FROM students
WHERE gpa BETWEEN 1.7 AND 3.2
