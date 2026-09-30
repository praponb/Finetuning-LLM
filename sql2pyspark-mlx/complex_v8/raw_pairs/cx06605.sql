SELECT grade_status AS category
FROM enrollments
WHERE TO_DATE(enrolled_at) < DATE '2025-02-18'
INTERSECT
SELECT major
FROM students
ORDER BY category
