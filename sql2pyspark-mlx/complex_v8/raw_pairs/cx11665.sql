SELECT term AS label
FROM enrollments
INTERSECT
SELECT grade_status
FROM enrollments
