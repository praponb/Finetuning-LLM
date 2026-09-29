SELECT 'student' AS source, id AS entity_id, enrolled_on AS event_date
FROM students
UNION
SELECT 'student_2' AS source, id, enrolled_on
FROM students
WHERE YEAR(enrolled_on) = 2025
ORDER BY source
