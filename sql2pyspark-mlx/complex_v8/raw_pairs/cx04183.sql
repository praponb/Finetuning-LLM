SELECT id AS key_id, cohort AS label
FROM students
WHERE UPPER(TRIM(student_name)) LIKE 'L%'
INTERSECT
SELECT student_id AS key_id, term AS label
FROM enrollments
ORDER BY label
