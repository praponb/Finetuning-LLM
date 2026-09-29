SELECT id AS entity_id
FROM students
INTERSECT ALL
SELECT id AS entity_id
FROM students
WHERE cohort = 'C2023'
ORDER BY entity_id
