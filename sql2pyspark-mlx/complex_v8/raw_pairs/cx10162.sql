SELECT grade_status
FROM enrollments
INTERSECT
SELECT term AS grade_status
FROM enrollments
ORDER BY grade_status
