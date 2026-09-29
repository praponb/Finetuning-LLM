SELECT id, credits, 'course' AS record_type
FROM courses
UNION
SELECT credits, id, 'course_2' AS record_type
FROM courses
