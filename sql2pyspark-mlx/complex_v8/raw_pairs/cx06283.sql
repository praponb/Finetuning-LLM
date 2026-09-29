SELECT 'student' AS record_type, id, cohort AS label
FROM students
UNION ALL
SELECT 'course' AS record_type, id, course_code
FROM courses
ORDER BY record_type, id
