SELECT id AS key_id
FROM students
WHERE gpa <= 3.4
INTERSECT ALL
SELECT credits
FROM courses
