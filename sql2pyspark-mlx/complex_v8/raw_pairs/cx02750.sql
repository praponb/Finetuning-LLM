SELECT id AS key_id, enrolled_on, gpa
FROM students
WHERE gpa < 3.4
  AND is_international = TRUE
UNION ALL
SELECT id, enrolled_on, gpa
FROM students
WHERE YEAR(enrolled_on) = 2024
ORDER BY enrolled_on DESC, key_id
