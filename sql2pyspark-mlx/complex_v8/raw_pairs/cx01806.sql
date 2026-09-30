SELECT student_name AS category, gpa AS amount, id AS entity_id
FROM students
WHERE gpa < 3.4
  AND is_international = FALSE
UNION ALL
SELECT cohort AS category, gpa AS amount, id AS entity_id
FROM students
WHERE LENGTH(TRIM(student_name)) >= 8
ORDER BY entity_id
