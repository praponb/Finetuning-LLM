SELECT id AS ref_id
FROM students
INTERSECT
SELECT id AS ref_id
FROM enrollments
ORDER BY ref_id
