SELECT student_name AS label, enrolled_on
FROM students
UNION
SELECT cohort AS label, enrolled_on
FROM students
ORDER BY label DESC, enrolled_on
