SELECT cohort, id, major
FROM students
WHERE is_international = FALSE
UNION ALL
SELECT student_name, id, cohort
FROM students
ORDER BY id
