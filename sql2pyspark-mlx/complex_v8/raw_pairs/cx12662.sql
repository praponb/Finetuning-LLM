SELECT student_name, major AS label
FROM students
WHERE LENGTH(TRIM(student_name)) >= 6
UNION ALL
SELECT term AS student_name, grade_status AS label
FROM enrollments
WHERE course_id >= 9
