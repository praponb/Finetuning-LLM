SELECT course_id, student_id AS ref_id, 'enrollment' AS source
FROM enrollments
UNION ALL
SELECT id AS course_id, student_id AS ref_id, 'enrollment_2' AS source
FROM enrollments
WHERE HOUR(enrolled_at) BETWEEN 10 AND 14
UNION ALL
SELECT id AS course_id, credits AS ref_id, 'course' AS source
FROM courses
