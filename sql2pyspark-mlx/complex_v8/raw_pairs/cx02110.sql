SELECT course_id AS entity_id, term AS label
FROM enrollments
UNION
SELECT id, cohort
FROM students
