SELECT id AS ref_id
FROM students
WHERE gpa <= 3.2
INTERSECT
SELECT id
FROM students
WHERE NOT is_international
ORDER BY ref_id
