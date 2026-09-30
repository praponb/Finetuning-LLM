SELECT student_id AS qty
FROM enrollments
WHERE score <= 59.8
  AND grade_status = 'INCOMPLETE'
INTERSECT
SELECT credits AS qty
FROM courses
