SELECT student_id
FROM enrollments
MINUS
SELECT id
FROM students
WHERE NOT is_international
  AND major = 'Bio'
ORDER BY student_id DESC
