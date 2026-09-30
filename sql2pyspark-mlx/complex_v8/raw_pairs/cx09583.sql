SELECT id
FROM students
WHERE gpa >= 2.5
INTERSECT
SELECT student_id AS id
FROM enrollments
