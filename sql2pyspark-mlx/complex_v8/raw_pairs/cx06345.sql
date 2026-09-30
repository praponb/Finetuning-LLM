SELECT id AS ref_id
FROM courses
WHERE credits <= 3
INTERSECT ALL
SELECT id
FROM students
WHERE gpa > 2.0
ORDER BY ref_id DESC
