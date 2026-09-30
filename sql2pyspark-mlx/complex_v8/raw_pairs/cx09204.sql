SELECT course_id
FROM enrollments
INTERSECT
SELECT id
FROM students
ORDER BY course_id DESC
