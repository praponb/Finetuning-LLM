SELECT id
FROM students
WHERE cohort IN ('C2022', 'C2024', 'C2021')
  AND gpa > 2.7
EXCEPT
SELECT student_id
FROM enrollments
ORDER BY id
