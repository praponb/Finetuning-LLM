SELECT student_id AS key_id
FROM enrollments
WHERE score < 57.0
  AND course_id <= 2
EXCEPT
SELECT course_id AS key_id
FROM enrollments
ORDER BY key_id
