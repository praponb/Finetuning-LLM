SELECT student_id AS qty
FROM enrollments
MINUS
SELECT credits AS qty
FROM courses
