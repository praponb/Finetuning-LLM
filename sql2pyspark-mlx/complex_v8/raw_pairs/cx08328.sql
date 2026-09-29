SELECT credits AS ref_id, id AS entity_id, 'course' AS record_type
FROM courses
WHERE dept <> 'Math'
UNION
SELECT id, student_id, 'enrollment' AS record_type
FROM enrollments
ORDER BY record_type DESC
