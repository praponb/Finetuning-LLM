SELECT student_id
FROM enrollments
WHERE enrolled_at >= TIMESTAMP '2025-02-06 06:00:00'
EXCEPT
SELECT id
FROM students
WHERE gpa >= 2.0
  AND major = 'Art'
