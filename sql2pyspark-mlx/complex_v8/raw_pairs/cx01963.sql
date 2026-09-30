SELECT id AS key_id
FROM enrollments
WHERE student_id <= 17
INTERSECT ALL
SELECT id
FROM students
WHERE enrolled_on BETWEEN DATE '2024-04-01' AND DATE '2025-06-27'
  AND cohort <> 'C2021'
ORDER BY key_id DESC
