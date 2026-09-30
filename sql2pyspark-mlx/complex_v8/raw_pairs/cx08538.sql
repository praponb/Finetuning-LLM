SELECT course_id
FROM enrollments
WHERE term NOT IN ('2025T1', '2024T2')
INTERSECT ALL
SELECT id AS course_id
FROM students
WHERE is_international = TRUE
  AND gpa >= 2.7
