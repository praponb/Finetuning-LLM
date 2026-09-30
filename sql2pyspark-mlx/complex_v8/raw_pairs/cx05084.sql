SELECT credits AS ref_id, id, 'course' AS record_type
FROM courses
UNION ALL
SELECT credits, id, 'course_2' AS record_type
FROM courses
WHERE course_code IS NOT NULL
