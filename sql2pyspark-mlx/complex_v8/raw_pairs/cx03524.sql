SELECT id AS key_id, enrolled_on AS event_date
FROM students
WHERE gpa <= 3.4
UNION ALL
SELECT id, enrolled_on
FROM students
ORDER BY key_id
