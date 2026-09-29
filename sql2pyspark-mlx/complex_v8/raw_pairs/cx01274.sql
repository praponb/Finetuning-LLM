SELECT course_id, grade_status AS label, 'enrollment' AS src
FROM enrollments
UNION ALL
SELECT student_id, term, 'enrollment_2' AS src
FROM enrollments
ORDER BY label, src
