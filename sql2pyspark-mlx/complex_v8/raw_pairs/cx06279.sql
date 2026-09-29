SELECT id AS ref_id, gpa, enrolled_on AS day
FROM students
UNION
SELECT id AS ref_id, gpa, enrolled_on AS day
FROM students
ORDER BY gpa
