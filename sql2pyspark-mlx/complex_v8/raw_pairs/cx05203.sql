SELECT id AS entity_id, dept
FROM courses
INTERSECT
SELECT id, grade_status
FROM enrollments
