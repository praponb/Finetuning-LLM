SELECT term
FROM enrollments
WHERE student_id >= 4
INTERSECT
SELECT student_name AS term
FROM students
WHERE gpa BETWEEN 2.5 AND 3.2
  AND LOWER(student_name) LIKE '% n%'
ORDER BY term DESC
