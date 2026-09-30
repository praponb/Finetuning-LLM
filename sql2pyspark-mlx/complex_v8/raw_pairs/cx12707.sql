SELECT id AS entity_id
FROM enrollments
WHERE term = '2024T1'
  AND course_id >= 5
EXCEPT ALL
SELECT id
FROM courses
ORDER BY entity_id DESC
