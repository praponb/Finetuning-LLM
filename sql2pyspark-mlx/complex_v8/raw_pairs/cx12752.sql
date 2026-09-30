SELECT id AS entity_id, enrolled_on AS day
FROM students
UNION ALL
SELECT id AS entity_id, enrolled_on AS day
FROM students
WHERE YEAR(enrolled_on) = 2024
  AND cohort IS NOT NULL
UNION ALL
SELECT id AS entity_id, enrolled_on AS day
FROM students
WHERE major IN ('Art', 'Bio')
ORDER BY entity_id
