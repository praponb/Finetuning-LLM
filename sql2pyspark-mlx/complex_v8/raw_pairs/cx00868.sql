SELECT course_id AS entity_id
FROM enrollments
WHERE score BETWEEN 23.7 AND 83.1
  AND term IN ('2025T1', '2024T1', '2025T2')
EXCEPT
SELECT id
FROM students
